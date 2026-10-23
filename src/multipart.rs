//! A multipart request body read as it arrives, the way
//! couch_httpd:parse_multipart_request/3 reads one: whatever precedes the
//! first boundary is skipped, a part's headers run to a blank line and
//! its body to the next boundary, and a boundary followed by "--" ends
//! it. Nothing after that is looked at.

use crate::error::{CouchError, Result};
use axum::body::BodyDataStream;
use bytes::{Buf, Bytes, BytesMut};
use futures::StreamExt;
use memchr::memmem::Finder;

/// A part's headers: names lowercased, values as sent.
pub type Headers = Vec<(String, String)>;

pub struct Multipart {
    body: BodyDataStream,
    buf: BytesMut,
    /// "--" and the boundary: what opens the first part.
    open: Finder<'static>,
    /// CRLF, "--" and the boundary: what ends a part's body.
    delim: Finder<'static>,
    state: State,
}

#[derive(PartialEq)]
enum State {
    /// Before the first boundary.
    Start,
    /// In a part's body.
    Body,
    /// Past the boundary that ended a part's body.
    Between,
    /// Past the last one.
    End,
}

fn incomplete() -> CouchError {
    CouchError::bad_request("incomplete multipart stream")
}

/// How many bytes at the end of `buf` could be the start of `pat`.
fn partial(buf: &[u8], pat: &[u8]) -> usize {
    (1..pat.len().min(buf.len() + 1)).rev().find(|&k| buf.ends_with(&pat[..k])).unwrap_or(0)
}

impl Multipart {
    pub fn new(body: BodyDataStream, boundary: &str) -> Multipart {
        Multipart {
            body,
            buf: BytesMut::new(),
            open: Finder::new(format!("--{boundary}").as_bytes()).into_owned(),
            delim: Finder::new(format!("\r\n--{boundary}").as_bytes()).into_owned(),
            state: State::Start,
        }
    }

    /// Read more of the body; false at its end.
    async fn fill(&mut self) -> Result<bool> {
        match self.body.next().await {
            Some(Ok(b)) => {
                self.buf.extend_from_slice(&b);
                Ok(true)
            }
            Some(Err(e)) => Err(crate::http::body_error(e)),
            None => Ok(false),
        }
    }

    /// The next part's headers, skipping what is left of the current
    /// part; None past the last part.
    pub async fn next_part(&mut self) -> Result<Option<Headers>> {
        match self.state {
            State::Start => loop {
                if let Some(i) = self.open.find(&self.buf) {
                    self.buf.advance(i + self.open.needle().len());
                    break;
                }
                let keep = self.open.needle().len() - 1;
                if self.buf.len() > keep {
                    self.buf.advance(self.buf.len() - keep);
                }
                if !self.fill().await? {
                    return Err(incomplete());
                }
            },
            State::Body | State::Between => {
                while self.body_chunk().await?.is_some() {}
                // check_for_last/1
                while self.buf.len() < 2 {
                    if !self.fill().await? {
                        return Err(incomplete());
                    }
                }
                if self.buf.starts_with(b"--") {
                    self.state = State::End;
                    return Ok(None);
                }
            }
            State::End => return Ok(None),
        }
        // parse_part_header/1: what is left of the boundary's line, then
        // header lines, up to a blank line.
        let mut from = 0;
        let end = loop {
            if let Some(i) = memchr::memmem::find(&self.buf[from..], b"\r\n\r\n") {
                break from + i;
            }
            from = self.buf.len().saturating_sub(3);
            if !self.fill().await? {
                return Err(incomplete());
            }
        };
        let block = String::from_utf8_lossy(&self.buf.split_to(end + 4)[..end]).into_owned();
        let mut headers = Vec::new();
        for line in block.split("\r\n").filter(|l| !l.is_empty()) {
            let (name, value) = line.split_once(':').ok_or_else(|| CouchError::bad_request("bad part header"))?;
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
        self.state = State::Body;
        Ok(Some(headers))
    }

    /// The next piece of the current part's body; None at its end.
    pub async fn body_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.state != State::Body {
            return Ok(None);
        }
        loop {
            if let Some(i) = self.delim.find(&self.buf) {
                if i > 0 {
                    return Ok(Some(self.buf.split_to(i).freeze()));
                }
                self.buf.advance(self.delim.needle().len());
                self.state = State::Between;
                return Ok(None);
            }
            // All but what may be the start of the boundary.
            let n = self.buf.len() - partial(&self.buf, self.delim.needle());
            if n > 0 {
                return Ok(Some(self.buf.split_to(n).freeze()));
            }
            if !self.fill().await? {
                return Err(incomplete());
            }
        }
    }

    /// A part's whole body.
    pub async fn body(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(b) = self.body_chunk().await? {
            out.extend_from_slice(&b);
        }
        Ok(out)
    }

    /// Read the request through, ignoring what is in it, as couch_doc's
    /// WaitFun waits for the parser to finish.
    pub async fn drain(&mut self) {
        while let Ok(Some(_)) = self.next_part().await {}
        while let Some(Ok(_)) = self.body.next().await {}
    }
}
