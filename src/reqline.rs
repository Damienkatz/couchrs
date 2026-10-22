//! Request targets as mochiweb takes them. Clients (curl, and CouchDB's
//! own documentation) send `?startkey="a"` with the quotes raw, which
//! CouchDB accepts and hyper's URI parser does not. This sits between the
//! connection and hyper and percent-encodes such bytes in each HTTP/1
//! request line, following the message framing (Content-Length and
//! chunked bodies pass untouched; HTTP/2 and upgraded connections pass
//! through whole).

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Heads longer than this are left for hyper to refuse.
const MAX_HEAD: usize = 1 << 20;

enum State {
    /// Gathering a request's line and headers.
    Head,
    Body(u64),
    /// Within a chunked body: a size line, data, its CRLF, trailers.
    ChunkSize,
    ChunkData(u64),
    ChunkEnd(u8),
    Trailers,
    Through,
}

pub struct RequestLines<S> {
    inner: S,
    state: State,
    head: Vec<u8>,
    out: Vec<u8>,
    at: usize,
    line: Vec<u8>,
}

impl<S> RequestLines<S> {
    pub fn new(inner: S) -> RequestLines<S> {
        RequestLines { inner, state: State::Head, head: Vec::new(), out: Vec::new(), at: 0, line: Vec::new() }
    }
}

/// Bytes http::Uri refuses that mochiweb takes.
fn needs_encoding(b: u8) -> bool {
    matches!(b, b'"' | b'<' | b'>' | b'\\' | b'^' | b'`' | b'{' | b'|' | b'}') || b >= 0x80
}

/// The request line with its target's unacceptable bytes encoded.
fn fix_line(line: &[u8]) -> Vec<u8> {
    let first = line.iter().position(|b| *b == b' ');
    let last = line.iter().rposition(|b| *b == b' ');
    let (Some(f), Some(l)) = (first, last) else { return line.to_vec() };
    if f >= l || !line[f + 1..l].iter().any(|b| needs_encoding(*b)) {
        return line.to_vec();
    }
    let mut out = Vec::with_capacity(line.len() + 16);
    out.extend_from_slice(&line[..=f]);
    for &b in &line[f + 1..l] {
        if needs_encoding(b) {
            out.extend_from_slice(format!("%{b:02X}").as_bytes());
        } else {
            out.push(b);
        }
    }
    out.extend_from_slice(&line[l..]);
    out
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

impl<S> RequestLines<S> {
    /// Feed bytes read from the connection; what hyper should see goes to
    /// `out`.
    fn feed(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            match self.state {
                State::Through => {
                    self.out.extend_from_slice(data);
                    return;
                }
                State::Head => {
                    let start = self.head.len().saturating_sub(3);
                    self.head.extend_from_slice(data);
                    data = &[];
                    // Blank lines before a request are skipped, as by httparse.
                    let lead = self.head.iter().take_while(|b| **b == b'\r' || **b == b'\n').count();
                    if self.head[lead..].starts_with(b"PRI * HTTP/2.0") {
                        self.state = State::Through;
                        data = &[];
                        self.out.append(&mut self.head);
                        continue;
                    }
                    let Some(end) = find(&self.head[start.max(lead)..], b"\r\n\r\n").map(|i| i + start.max(lead) + 4) else {
                        if self.head.len() > MAX_HEAD {
                            self.state = State::Through;
                            self.out.append(&mut self.head);
                        }
                        continue;
                    };
                    let head: Vec<u8> = self.head.drain(..end).collect();
                    let rest = std::mem::take(&mut self.head);
                    let line_end = find(&head[lead..], b"\r\n").map(|i| i + lead).unwrap_or(head.len());
                    if &head[lead..line_end] == b"PRI * HTTP/2.0" {
                        // HTTP/2's preface: the rest is frames.
                        self.state = State::Through;
                        self.out.extend_from_slice(&head);
                        self.out.extend_from_slice(&rest);
                        return;
                    }
                    self.out.extend_from_slice(&head[..lead]);
                    self.out.extend_from_slice(&fix_line(&head[lead..line_end]));
                    self.out.extend_from_slice(&head[line_end..]);
                    let (mut len, mut chunked, mut upgrade) = (0u64, false, false);
                    for h in head[line_end..].split(|b| *b == b'\n') {
                        let h = String::from_utf8_lossy(h);
                        let Some((k, v)) = h.split_once(':') else { continue };
                        let (k, v) = (k.trim().to_ascii_lowercase(), v.trim().to_ascii_lowercase());
                        match k.as_str() {
                            "content-length" => len = v.parse().unwrap_or(0),
                            "transfer-encoding" => chunked = v.split(',').any(|t| t.trim() == "chunked"),
                            "upgrade" => upgrade = true,
                            _ => {}
                        }
                    }
                    let method = &head[lead..line_end];
                    self.state = if upgrade || method.starts_with(b"CONNECT ") {
                        State::Through
                    } else if chunked {
                        State::ChunkSize
                    } else if len > 0 {
                        State::Body(len)
                    } else {
                        State::Head
                    };
                    // What followed the head goes through the new state.
                    if !rest.is_empty() {
                        self.feed_owned(rest);
                    }
                }
                State::Body(n) => {
                    let take = (n.min(data.len() as u64)) as usize;
                    self.out.extend_from_slice(&data[..take]);
                    data = &data[take..];
                    self.state = if n - take as u64 == 0 { State::Head } else { State::Body(n - take as u64) };
                }
                State::ChunkSize | State::Trailers => {
                    let Some(i) = data.iter().position(|b| *b == b'\n') else {
                        self.line.extend_from_slice(data);
                        self.out.extend_from_slice(data);
                        return;
                    };
                    self.line.extend_from_slice(&data[..=i]);
                    self.out.extend_from_slice(&data[..=i]);
                    data = &data[i + 1..];
                    let line = std::mem::take(&mut self.line);
                    let text = String::from_utf8_lossy(&line);
                    let text = text.trim();
                    self.state = match self.state {
                        State::ChunkSize => {
                            let size = u64::from_str_radix(text.split(';').next().unwrap_or("").trim(), 16).unwrap_or(0);
                            if size == 0 { State::Trailers } else { State::ChunkData(size) }
                        }
                        _ if text.is_empty() => State::Head,
                        _ => State::Trailers,
                    };
                }
                State::ChunkData(n) => {
                    let take = (n.min(data.len() as u64)) as usize;
                    self.out.extend_from_slice(&data[..take]);
                    data = &data[take..];
                    self.state = if n - take as u64 == 0 { State::ChunkEnd(2) } else { State::ChunkData(n - take as u64) };
                }
                State::ChunkEnd(k) => {
                    let take = (k as usize).min(data.len());
                    self.out.extend_from_slice(&data[..take]);
                    data = &data[take..];
                    self.state = if k as usize - take == 0 { State::ChunkSize } else { State::ChunkEnd(k - take as u8) };
                }
            }
        }
    }

    fn feed_owned(&mut self, data: Vec<u8>) {
        self.feed(&data);
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for RequestLines<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        loop {
            if this.at < this.out.len() {
                let n = (this.out.len() - this.at).min(buf.remaining());
                buf.put_slice(&this.out[this.at..this.at + n]);
                this.at += n;
                if this.at == this.out.len() {
                    this.out.clear();
                    this.at = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if matches!(this.state, State::Through) && this.head.is_empty() {
                return Pin::new(&mut this.inner).poll_read(cx, buf);
            }
            // A sized body is read straight into the caller's buffer.
            if let State::Body(n) = this.state
                && this.head.is_empty()
            {
                let limit = (n.min(buf.remaining() as u64)) as usize;
                let k = {
                    let mut sub = buf.take(limit);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut sub) {
                        Poll::Ready(Ok(())) => sub.filled().len(),
                        other => return other,
                    }
                };
                // SAFETY: the inner reader initialized and filled k bytes.
                unsafe { buf.assume_init(k) };
                buf.advance(k);
                if k > 0 {
                    this.state = if n == k as u64 { State::Head } else { State::Body(n - k as u64) };
                }
                return Poll::Ready(Ok(()));
            }
            let mut tmp = [0u8; 16 * 1024];
            let mut rb = ReadBuf::new(&mut tmp);
            match Pin::new(&mut this.inner).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => {
                    let got = rb.filled();
                    if got.is_empty() {
                        // End of input: whatever head was gathered goes on.
                        let rest = std::mem::take(&mut this.head);
                        if rest.is_empty() {
                            return Poll::Ready(Ok(()));
                        }
                        this.out.extend_from_slice(&rest);
                        continue;
                    }
                    let got = got.to_vec();
                    this.feed(&got);
                }
                other => return other,
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for RequestLines<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[std::io::IoSlice<'_>]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(chunks: &[&[u8]]) -> Vec<u8> {
        let mut r = RequestLines::new(());
        for c in chunks {
            r.feed(c);
        }
        let mut out = r.out;
        out.extend(r.head);
        out
    }

    #[test]
    fn encodes_targets_not_bodies() {
        let req = b"GET /db/_all_docs?startkey=\"a\" HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(run(&[req]), b"GET /db/_all_docs?startkey=%22a%22 HTTP/1.1\r\nHost: x\r\n\r\n");
        // A body that looks like a request is left alone; the request
        // after it is fixed.
        let two = b"POST /db HTTP/1.1\r\nContent-Length: 27\r\n\r\nGET /\"x\" HTTP/1.1\r\n\r\n12345GET /\"y\" HTTP/1.1\r\n\r\n";
        assert_eq!(run(&[&two[..30], &two[30..]]), b"POST /db HTTP/1.1\r\nContent-Length: 27\r\n\r\nGET /\"x\" HTTP/1.1\r\n\r\n12345GET /%22y%22 HTTP/1.1\r\n\r\n");
        let chunked = b"PUT /d HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n\"x\"\r\n\r\n0\r\n\r\nGET /\"z\" HTTP/1.1\r\n\r\n";
        assert_eq!(run(&[chunked]), b"PUT /d HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n\"x\"\r\n\r\n0\r\n\r\nGET /%22z%22 HTTP/1.1\r\n\r\n");
        let h2 = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\"\"";
        assert_eq!(run(&[h2]), h2);
    }
}
