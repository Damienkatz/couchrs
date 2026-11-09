#!/usr/bin/env python3
"""Write the AWS benchmark as Markdown that GitHub shows directly:
report_md.py [results-aws.json] [aws/report.md]

GitHub renders Markdown, tables and SVG images but runs no scripts, so
each chart of report-aws.html is drawn here as a static SVG (light and
dark colors by prefers-color-scheme, on a card of its own so it reads on
either GitHub theme) beside a table of its numbers. Only Apache CouchDB
and CouchRS's hybrid engine are shown.
"""
import datetime
import html
import json
import math
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(here, "results-aws.json")
dst = sys.argv[2] if len(sys.argv) > 2 else os.path.join(here, "aws", "report.md")
chart_dir = os.path.join(os.path.dirname(dst), "report")
os.makedirs(chart_dir, exist_ok=True)

results = json.load(open(src))
E, H = results["erlang"], results["hybrid"]
# key, name, CSS class of its color
SERIES = [("erlang", "CouchDB (Erlang)", "s-erlang"), ("hybrid", "CouchRS (hybrid)", "s-couchrs")]
SRC = {"erlang": E, "hybrid": H}


# ------------------------------------------------------------ formatting
def compact(v):
    if v is None or not math.isfinite(v):
        return "–"
    a = abs(v)
    if a >= 1e9:
        return f"{v / 1e9:.{0 if a >= 1e10 else 1}f}B"
    if a >= 1e6:
        return f"{v / 1e6:.{0 if a >= 1e7 else 1}f}M"
    if a >= 1e4:
        return f"{v / 1e3:.0f}K"
    if a >= 1e3:
        return f"{v / 1e3:.1f}K"
    if a >= 100:
        return f"{v:.0f}"
    if a >= 10:
        return f"{v:.1f}"
    if a >= 1:
        return f"{v:.2f}"
    if a == 0:
        return "0"
    return f"{v:.2g}"


def nbytes(v):
    if v is None:
        return "–"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while v >= 1000 and i < len(units) - 1:
        v /= 1000
        i += 1
    return f"{v:.0f} {units[i]}" if v >= 100 else f"{v:.1f} {units[i]}" if v >= 10 else f"{v:.2f} {units[i]}"


def dur(s):
    if s is None:
        return "–"
    if s < 60:
        return f"{s:.1f} s"
    if s < 3600:
        return f"{s / 60:.1f} min"
    return f"{s / 3600:.2f} h"


def ms(v):
    if v is None:
        return "–"
    return f"{v:.2f}" if v < 10 else f"{v:.1f}" if v < 100 else f"{v:.0f}"


def secs(v):
    return f"{v:.2f}" if v < 10 else f"{v:.0f}"


def get(d, path):
    for k in path.split("."):
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def speed(e, r, lower_better):
    """CouchRS's speed relative to CouchDB, in words."""
    if not e or not r:
        return "–"
    x = e / r if lower_better else r / e
    return f"{x:.2f}× faster" if x >= 1 else f"{1 / x:.2f}× slower"


# ------------------------------------------------------------------ SVG
STYLE = """
svg{font-family:system-ui,-apple-system,"Segoe UI",Helvetica,Arial,sans-serif}
.bg{fill:#fcfcfb;stroke:rgba(11,11,11,.12)}
.title{font-size:15px;font-weight:600;fill:#0b0b0b}
.sub{font-size:12px;fill:#6f6e69}
.leg{font-size:12.5px;fill:#52514e}
.lbl{font-size:12.5px;fill:#52514e}
.val{font-size:11.5px;fill:#52514e;font-variant-numeric:tabular-nums}
.ratio{font-size:12px;font-weight:600;fill:#0b0b0b;font-variant-numeric:tabular-nums}
.tick{font-size:11px;fill:#6f6e69;font-variant-numeric:tabular-nums}
.grid{stroke:#e1e0d9}
.axis{stroke:#c3c2b7}
.s-erlang{fill:#2a78d6}
.s-couchrs{fill:#eda100}
@media (prefers-color-scheme:dark){
.bg{fill:#1a1a19;stroke:rgba(255,255,255,.12)}
.title,.ratio{fill:#f0efec}
.sub,.tick{fill:#9a9890}
.leg,.lbl,.val{fill:#c3c2b7}
.grid{stroke:#2c2c2a}
.axis{stroke:#45453f}
.s-erlang{fill:#3987e5}
.s-couchrs{fill:#c98500}
}
"""
W = 760
PAD = 18


def esc(s):
    return html.escape(str(s), quote=True)


def nice_ticks(mx):
    step = 10 ** math.floor(math.log10(mx))
    m = mx / step
    s = step / 2 if m <= 2 else step if m <= 5 else step * 2
    ticks, t = [], 0.0
    while t <= mx * 1.0001:
        ticks.append(t)
        t += s
    if ticks[-1] < mx:
        ticks.append(ticks[-1] + s)
    return ticks


def header(out, title, sub, series):
    """Title, subtitle and (for two series) a legend; returns the y below them."""
    y = PAD + 15
    out.append(f'<text x="{PAD}" y="{y}" class="title">{esc(title)}</text>')
    y += 19
    out.append(f'<text x="{PAD}" y="{y}" class="sub">{esc(sub)}</text>')
    if len(series) > 1:
        y += 22
        x = PAD
        for _, name, cls in series:
            out.append(f'<rect x="{x}" y="{y - 10}" width="10" height="10" rx="2" class="{cls}"/>')
            out.append(f'<text x="{x + 16}" y="{y}" class="leg">{esc(name)}</text>')
            x += 16 + 7.2 * len(name) + 22
    return y + 16


def write_svg(name, height, body, label):
    path = os.path.join(chart_dir, name + ".svg")
    with open(path, "w") as f:
        f.write(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" viewBox="0 0 {W} {height}" '
                f'role="img" aria-label="{esc(label)}">\n<style>{STYLE}</style>\n'
                f'<rect x="0.5" y="0.5" width="{W - 1}" height="{height - 1}" rx="10" class="bg"/>\n')
        f.write("\n".join(body))
        f.write("\n</svg>\n")
    return f"report/{name}.svg"


def bar_path(x0, xe, y, h):
    """A bar from the baseline x0 to xe, its data end rounded 4px."""
    r = min(4, (xe - x0) / 2, h / 2)
    return (f"M{x0:.1f},{y:.1f} H{xe - r:.1f} Q{xe:.1f},{y:.1f} {xe:.1f},{y + r:.1f} V{y + h - r:.1f} "
            f"Q{xe:.1f},{y + h:.1f} {xe - r:.1f},{y + h:.1f} H{x0:.1f} Z")


def bars(name, title, sub, rows, fmt=compact, unit="", lower_better=False, label_w=96, extra_cols=None):
    """A grouped bar chart and its table. rows: [(label, {series key: value})]."""
    ser = [s for s in SERIES if any(v.get(s[0]) is not None for _, v in rows)]
    rows = [(l, v) for l, v in rows if any(v.get(s[0]) is not None for s in ser)]
    vals = [v[s[0]] for _, v in rows for s in ser if v.get(s[0]) is not None and v[s[0]] > 0]
    out = []
    top = header(out, title, sub, ser)
    ticks = nice_ticks(max(vals))
    hi = ticks[-1]
    bar_h, gap = 11, 2
    row_h = len(ser) * bar_h + (len(ser) - 1) * gap + 14
    x0, x1 = PAD + label_w, W - PAD - 54
    sx = lambda v: x0 + v / hi * (x1 - x0)
    plot_top = top + 4
    plot_bottom = plot_top + len(rows) * row_h
    for t in ticks:
        x = sx(t)
        out.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{plot_top - 2}" y2="{plot_bottom + 2}" class="grid"/>')
        # Round ticks of plain numbers need no decimals.
        tick = f"{t:g}" if fmt in (ms, secs) else fmt(t)
        out.append(f'<text x="{x:.1f}" y="{plot_bottom + 18}" text-anchor="middle" class="tick">{esc(tick)}</text>')
    out.append(f'<line x1="{x0}" x2="{x0}" y1="{plot_top - 2}" y2="{plot_bottom + 2}" class="axis"/>')
    for i, (label, v) in enumerate(rows):
        y = plot_top + i * row_h + 7
        out.append(f'<text x="{x0 - 10}" y="{y + bar_h + 5}" text-anchor="end" class="lbl">{esc(label)}</text>')
        for si, (key, _, cls) in enumerate(ser):
            val = v.get(key)
            if val is None:
                continue
            by = y + si * (bar_h + gap)
            xe = max(sx(val), x0 + 4)
            out.append(f'<path d="{bar_path(x0, xe, by, bar_h)}" class="{cls}"/>')
            out.append(f'<text x="{xe + 6:.1f}" y="{by + bar_h - 1.5:.1f}" class="val">{esc(fmt(val))}</text>')
    height = plot_bottom + 26 + PAD - 6
    img = write_svg(name, height, out, title)
    # The table: every value, and CouchRS's speed against CouchDB.
    u = f", {unit}" if unit else ""
    head = ["", *[n + u for _, n, _ in ser]]
    two = len(ser) == 2
    if two:
        head.append("CouchRS vs CouchDB")
    lines = ["| " + " | ".join(head) + " |", "|" + "|".join([":--"] + ["--:"] * (len(head) - 1)) + "|"]
    for label, v in rows:
        cells = [label, *[fmt(v[k]) if v.get(k) is not None else "–" for k, _, _ in ser]]
        if two:
            cells.append(speed(v.get("erlang"), v.get("hybrid"), lower_better))
        lines.append("| " + " | ".join(cells) + " |")
    return f"![{title}]({img})\n\n" + "\n".join(lines) + "\n"


def ratio_chart(name, rows):
    """CouchRS's speed relative to CouchDB, each bar starting at 1x.
    rows: [(label, ratio, CouchDB value text, CouchRS value text)]."""
    rows = [r for r in rows if r[1] and math.isfinite(r[1])]
    out = []
    top = header(out, "CouchRS (hybrid) speed relative to CouchDB", "each bar starts at 1× (parity); right of it, CouchRS is faster", [])
    ticks = nice_ticks(max(2, *[r[1] for r in rows]))
    hi = ticks[-1]
    row_h = 26
    x0, x1 = PAD + 176, W - PAD - 92
    sx = lambda v: x0 + v / hi * (x1 - x0)
    plot_top = top + 6
    plot_bottom = plot_top + len(rows) * row_h
    for t in ticks:
        x = sx(t)
        out.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{plot_top - 4}" y2="{plot_bottom + 2}" class="grid"/>')
        out.append(f'<text x="{x:.1f}" y="{plot_bottom + 18}" text-anchor="middle" class="tick">{t:g}×</text>')
    out.append(f'<line x1="{sx(1):.1f}" x2="{sx(1):.1f}" y1="{plot_top - 4}" y2="{plot_bottom + 2}" class="axis" stroke-width="1.5"/>')
    for i, (label, r, _, _) in enumerate(rows):
        y = plot_top + i * row_h + row_h / 2
        out.append(f'<text x="{x0 - 12}" y="{y + 4:.1f}" text-anchor="end" class="lbl">{esc(label)}</text>')
        a, b = sorted((sx(1), sx(r)))
        cls = "s-couchrs" if r >= 1 else "s-erlang"
        out.append(f'<rect x="{a:.1f}" y="{y - 5:.1f}" width="{max(b - a, 2):.1f}" height="10" rx="3" class="{cls}"/>')
        text = f"{r:.1f}×" if r >= 1 else f"{1 / r:.1f}× slower"
        if r >= 1:
            out.append(f'<text x="{b + 6:.1f}" y="{y + 4:.1f}" class="ratio">{esc(text)}</text>')
        else:
            out.append(f'<text x="{a - 6:.1f}" y="{y + 4:.1f}" text-anchor="end" class="ratio">{esc(text)}</text>')
    height = plot_bottom + 26 + PAD - 6
    img = write_svg(name, height, out, "CouchRS speed relative to CouchDB")
    lines = ["| Benchmark | CouchDB (Erlang) | CouchRS (hybrid) | CouchRS speed |", "|:--|--:|--:|--:|"]
    for label, r, ev, rv in rows:
        lines.append(f"| {label} | {ev} | {rv} | {r:.2f}× |")
    return f"![CouchRS speed relative to CouchDB]({img})\n\n" + "\n".join(lines) + "\n"


def row(label, f):
    return (label, {k: f(d) for k, d in SRC.items()})


# ---------------------------------------------------------------- report
CLASSES = ["100B", "1KB", "10KB", "100KB", "1MB", "10MB", "100MB", "1GB"]
SIZE = {"100B": "100 B", "1KB": "1 KB", "10KB": "10 KB", "100KB": "100 KB", "1MB": "1 MB", "10MB": "10 MB", "100MB": "100 MB", "1GB": "1 GB"}
by_class = lambda phase, field: [row(SIZE[c], lambda d, c=c: get(d, f"{phase}.{c}.{field}")) for c in CLASSES]
QUERIES = [("key_lookup_limit20", "Key lookup"), ("key_reduce", "Key + reduce"),
           ("group_level1_reduce", "Group reduce"), ("include_docs_limit10", "include_docs")]


def run_date():
    """The day the run ended, from the latest timestamp in the results."""
    stamps = []

    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k.endswith("_unix") and isinstance(v, (int, float)):
                    stamps.append(v)
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(E)
    walk(H)
    return datetime.datetime.fromtimestamp(max(stamps), datetime.timezone.utc).date().isoformat()


srv = E["server"]
ds = srv["dataset_bytes"]
md = []
md.append("# Apache CouchDB 3.5.3 (Erlang) vs CouchRS, on AWS\n")
md.append(
    "Apache CouchDB 3.5.3 and CouchRS with its hybrid engine (RocksDB for documents, B-tree index files for views) "
    "ran at the same time on AWS (us-east-1a, one cluster placement group), each on its own i4i.4xlarge server with "
    f"its own c7i.4xlarge load generator, against byte-identical data: {nbytes(ds)} in {compact(srv['dataset_docs'])} "
    "documents sized from 100 bytes to 1 GB, filled with generated English-like text. Each was loaded, then measured "
    "for writes, reads, deletes, view indexing and queries, connection scaling, view compaction, replication and "
    "compaction.\n")
md.append(f"- **Dataset:** {nbytes(ds)}, {compact(srv['dataset_docs'])} documents\n"
          "- **Servers:** i4i.4xlarge (16 vCPU, 128 GB RAM, local NVMe), one per server\n"
          "- **Load generators:** c7i.4xlarge, one per server\n"
          f"- **Run:** {run_date()}\n")
md.append("Generated by `bench/report_md.py` from `bench/results-aws.json`. The interactive version, "
          "`bench/report-aws.html`, opens in a browser (GitHub shows it only as source) and also covers the two other "
          "CouchRS builds that ran alongside.\n")

HEADLINE = [
    ("Bulk load (MB/s)", "load.mb_per_sec", False, lambda v: compact(v) + " MB/s"),
    ("PUT 1 KB (ops/s)", "put.1KB.ops_per_sec", False, lambda v: compact(v) + "/s"),
    ("PUT 10 MB (MB/s)", "put.10MB.mb_per_sec", False, lambda v: compact(v) + " MB/s"),
    ("PUT 1 GB (MB/s)", "put.1GB.mb_per_sec", False, lambda v: compact(v) + " MB/s"),
    ("GET 1 KB (ops/s)", "get.1KB.ops_per_sec", False, lambda v: compact(v) + "/s"),
    ("GET 10 MB (MB/s)", "get.10MB.mb_per_sec", False, lambda v: compact(v) + " MB/s"),
    ("DELETE 1 KB (ops/s)", "delete.1KB.ops_per_sec", False, lambda v: compact(v) + "/s"),
    ("View build (docs/s)", "view.docs_per_sec", False, lambda v: compact(v) + "/s"),
    ("View key lookup (q/s)", "view.queries.key_lookup_limit20.ops_per_sec", False, lambda v: compact(v) + "/s"),
    ("View reduce (q/s)", "view.queries.group_level1_reduce.ops_per_sec", False, lambda v: compact(v) + "/s"),
    ("Replication (time)", "replicate.secs", True, dur),
    ("Compaction (time)", "compact.secs", True, dur),
    ("View compaction (time)", "view_compact.secs", True, dur),
]
ratio_rows = []
for label, path, lower, f in HEADLINE:
    ev, rv = get(E, path), get(H, path)
    if ev and rv:
        ratio_rows.append((label, ev / rv if lower else rv / ev, f(ev), f(rv)))
md.append("## Summary\n")
md.append("How many times faster CouchRS is than CouchDB on each headline measure. Right of 1× means CouchRS is faster; "
          "left means CouchDB is faster.\n")
md.append(ratio_chart("summary", ratio_rows))

md.append("## Bulk load\n")
md.append("The whole dataset, loaded with 16 concurrent connections: small docs through `_bulk_docs` (1,000 per batch), "
          f"large ones as streamed single-document uploads. CouchDB: {nbytes(E['load']['bytes'])} in {dur(E['load']['secs'])}. "
          f"CouchRS: {nbytes(H['load']['bytes'])} in {dur(H['load']['secs'])}.\n")
md.append(bars("load-mb", "Load throughput", "MB/s · higher is better", [row("Throughput", lambda d: get(d, "load.mb_per_sec"))], unit="MB/s"))
md.append(bars("load-docs", "Load rate", "docs/s · higher is better", [row("Documents", lambda d: get(d, "load.docs_per_sec"))], unit="docs/s"))

md.append("## Writes\n")
md.append("Single-document PUTs of exact sizes, written into the loaded database. Docs up to 64 KB are JSON; larger ones "
          "carry the bytes as one attachment, uploaded as `multipart/related`.\n")
md.append(bars("put-ops", "PUT operations", "ops/s by doc size · higher is better", by_class("put", "ops_per_sec"), unit="ops/s"))
md.append(bars("put-mb", "PUT throughput", "MB/s by doc size · higher is better", by_class("put", "mb_per_sec"), unit="MB/s"))

md.append("## Reads\n")
md.append("Random GETs of documents (or their attachment) drawn from the loaded dataset in each size band. The dataset "
          "is larger than RAM, so many reads go to disk.\n")
md.append(bars("get-ops", "GET operations", "ops/s by size band · higher is better", by_class("get", "ops_per_sec"), unit="ops/s"))
md.append(bars("get-mb", "GET throughput", "MB/s by size band · higher is better", by_class("get", "mb_per_sec"), unit="MB/s"))

md.append("## Deletes\n")
md.append("Deleting the documents created by the write test, with their current revision.\n")
md.append(bars("delete-ops", "DELETE operations", "ops/s by doc size · higher is better", by_class("delete", "ops_per_sec"), unit="ops/s"))

md.append("## Latency\n")
md.append("Per-request latency from the client's side, measured for every operation in the tests above. The charts show "
          "p99 (nearest rank); the table after them has the mean and every percentile. Lower is better.\n")
lat = dict(fmt=ms, unit="ms", lower_better=True)
md.append(bars("latency-put", "PUT latency, p99", "ms · lower is better", by_class("put", "p99_ms"), **lat))
md.append(bars("latency-get", "GET latency, p99", "ms · lower is better", by_class("get", "p99_ms"), **lat))
md.append(bars("latency-delete", "DELETE latency, p99", "ms · lower is better", by_class("delete", "p99_ms"), **lat))
md.append(bars("latency-view", "View query latency, p99", "ms · lower is better",
               [row(l, lambda d, k=k: get(d, f"view.queries.{k}.p99_ms")) for k, l in QUERIES], label_w=110, **lat))
ops_rows = ["| Operation | Server | ops/s | MB/s | mean | p50 | p90 | p95 | p99 | max |", "|:--|:--|--:|--:|--:|--:|--:|--:|--:|--:|"]
ops = [(f"{ph.upper()} {SIZE[c]}", f"{ph}.{c}") for ph in ("put", "get", "delete") for c in CLASSES]
ops += [(f"View: {l}", f"view.queries.{k}") for k, l in QUERIES]
for label, path in ops:
    for key, name, _ in SERIES:
        m = get(SRC[key], path)
        if not m:
            continue
        ops_rows.append(f"| {label} | {name} | {compact(m['ops_per_sec'])} | {compact(m['mb_per_sec']) if m.get('bytes') else '–'} | "
                        f"{ms(m['mean_ms'])} | {ms(m['p50_ms'])} | {ms(m['p90_ms'])} | {ms(m['p95_ms'])} | {ms(m['p99_ms'])} | {ms(m['max_ms'])} |")
md.append("<details>\n<summary>Every operation: throughput, and latency in ms</summary>\n\n" + "\n".join(ops_rows) + "\n\n</details>\n")

md.append("## Views\n")
md.append("A JavaScript map function (`emit([doc.band, doc.group], doc.size)`) with the built-in `_sum` reduce, built over "
          "every document, then queried. Both servers run map functions in QuickJS.\n")
md.append(bars("view-build", "View build rate", "docs indexed per second · higher is better",
               [row("Index build", lambda d: get(d, "view.docs_per_sec"))], unit="docs/s"))
md.append(bars("view-queries", "View queries", "queries/s · higher is better",
               [row(l, lambda d, k=k: get(d, f"view.queries.{k}.ops_per_sec")) for k, l in QUERIES], unit="q/s", label_w=110))

sc = E["scale"]
steps = lambda d: {s["clients"]: s for s in get(d, "scale.steps") or []}
counts = sorted(set(steps(E)) | set(steps(H)))


def by_clients(f):
    return [(compact(c), {k: (f(steps(d)[c]) if c in steps(d) else None) for k, d in SRC.items()}) for c in counts]


def p99(op):
    # A step with no successful requests of a kind has no latency for it.
    return lambda st: st["ops"][op]["p99_ms"] if st["ops"][op]["ops"] else None


md.append("## Connection scaling\n")
md.append(f"Simulated hosts are added one at a time, each holding {compact(sc['per_host'])} keep-alive connections from its "
          f"own private address, up to {compact(sc['per_host'] * len(sc['steps']))}. Every client runs a closed loop: it sends its "
          "next request as soon as the last one answers, choosing evenly among GET of a random "
          f"{compact(sc['doc_bytes'])}-byte document (from {compact(sc['pool_docs'])}), PUT of its own document, and GET of a random "
          f"{compact(sc['att_bytes'])}-byte attachment. After each host joins, the load settles for {sc['warm_secs']} s and is then "
          f"measured for {sc['measure_secs']} s. Each load generator is its own c7i.4xlarge in the server's availability zone; its "
          "six private addresses are the six hosts.\n")
md.append(bars("scale-rps", "Requests per second", "all three request types · higher is better", by_clients(lambda st: st["ops_per_sec"]), unit="req/s"))
md.append(bars("scale-memory", "Server memory", "peak physical footprint while measuring · lower is better",
               by_clients(lambda st: st["server_footprint_bytes"]), fmt=nbytes, lower_better=True))
md.append(bars("scale-get", "GET document latency, p99", "ms · lower is better", by_clients(p99("get")), **lat))
md.append(bars("scale-put", "PUT document latency, p99", "ms · lower is better · no bar: no PUT succeeded", by_clients(p99("put")), **lat))
md.append(bars("scale-attachment", "GET attachment latency, p99", "ms · lower is better", by_clients(p99("get_att")), **lat))
step_rows = ["| Server | Clients | req/s | GET p99 | PUT p99 | Attachment p99 | HTTP errors | Memory | CPU cores |",
             "|:--|--:|--:|--:|--:|--:|--:|--:|--:|"]
for key, name, _ in SERIES:
    for st in get(SRC[key], "scale.steps") or []:
        o = st["ops"]
        step_rows.append(f"| {name} | {compact(st['clients'])} | {compact(st['ops_per_sec'])} | {ms(p99('get')(st))} | {ms(p99('put')(st))} | "
                         f"{ms(p99('get_att')(st))} | {compact(st['http_errors'])} | {nbytes(st['server_footprint_bytes'])} | "
                         f"{st['server_cpu_cores']:.1f} |")
md.append("<details>\n<summary>Every step (latency in ms; errors counted during the measured window)</summary>\n\n" + "\n".join(step_rows) + "\n\n</details>\n")

md.append("## View compaction\n")
md.append("Incremental index updates followed by view compaction. 200,000 small documents are inserted, updated three times "
          "and then deleted, and the index is brought up to date after each round, so it ends with the rows it started with "
          "plus whatever dead space incremental updates leave. Then the index is compacted. CouchDB's background indexer "
          "updates its index while the churn is written, so its index update times are not shown.\n")
upd = lambda d: sum(r["index_update_secs"] for r in d["view_compact"]["churn_rounds"]) / len(d["view_compact"]["churn_rounds"])
md.append(bars("view-update", "Incremental index update, CouchRS", "seconds to index 200,000 changed docs · lower is better",
               [("Per round", {"erlang": None, "hybrid": upd(H)})], fmt=secs, unit="s", lower_better=True))
md.append(bars("view-compact-time", "View compaction time", "seconds · lower is better",
               [row("Compaction", lambda d: get(d, "view_compact.secs"))], fmt=secs, unit="s", lower_better=True))
md.append(bars("view-compact-memory", "Extra memory during view compaction", "peak footprint over its level just before · lower is better",
               [row("Peak extra memory", lambda d: get(d, "view_compact.resources.footprint_peak_extra_bytes"))], fmt=nbytes, lower_better=True, label_w=130))
md.append(bars("view-compact-disk", "Extra disk during view compaction", "peak growth of the index directory · lower is better",
               [row("Peak extra disk", lambda d: get(d, "view_compact.resources.disk_peak_extra_bytes"))], fmt=nbytes, lower_better=True, label_w=130))
vc_rows = ["| Server | File at start | File before | Live before | File after | Reclaimed | Time |", "|:--|--:|--:|--:|--:|--:|--:|"]
for key, name, _ in SERIES:
    m = SRC[key]["view_compact"]
    vc_rows.append(f"| {name} | {nbytes(m['file_bytes_start'])} | {nbytes(m['file_bytes_before'])} | {nbytes(m['active_bytes_before'])} | "
                   f"{nbytes(m['file_bytes_after'])} | {nbytes(m['reclaimed_bytes'])} | {dur(m['secs'])} |")
md.append("Index file sizes: start is the index before the churn; before and after bracket the compaction.\n\n" + "\n".join(vc_rows) + "\n")

md.append("## Replication\n")
md.append("One full replication of the loaded database into a new database on the same server, driven by CouchDB's own "
          "replicator in both cases. That replicator talks to CouchRS over HTTP, so this also shows the two interoperate.\n")
mins = lambda v: None if v is None else v / 60
md.append(bars("replication-time", "Replication time", "minutes · lower is better",
               [row("Full copy", lambda d: mins(get(d, "replicate.secs")))], unit="min", lower_better=True))
md.append(bars("replication-mb", "Replication throughput", "MB/s · higher is better",
               [row("Throughput", lambda d: get(d, "replicate.mb_per_sec"))], unit="MB/s"))
md.append(bars("replication-docs", "Replication rate", "docs/s · higher is better",
               [row("Documents", lambda d: get(d, "replicate.docs_per_sec"))], unit="docs/s"))

md.append("## Compaction\n")
md.append("Compacting the loaded database to reclaim the space its dead revisions use. CouchDB copies the live documents "
          "and attachments into a new file and swaps it in. CouchRS drops dead revision bodies and attachment chunks, then "
          "has RocksDB compact its files.\n")
md.append(bars("compaction-time", "Compaction time", "minutes · lower is better",
               [row("Compaction", lambda d: mins(get(d, "compact.secs")))], unit="min", lower_better=True))
md.append(bars("compaction-rate", "Compaction throughput", "MB/s of live data · higher is better",
               [row("Live data", lambda d: get(d, "compact.live_mb_per_sec"))], unit="MB/s"))
md.append(bars("compaction-memory", "Peak memory during compaction", "server resident memory · lower is better",
               [row("Peak RAM", lambda d: get(d, "compact.resources.rss_peak_bytes"))], fmt=nbytes, lower_better=True))
md.append(bars("compaction-disk", "Peak extra disk during compaction", "growth of the data directory over its starting size · lower is better",
               [row("Peak extra disk", lambda d: get(d, "compact.resources.disk_peak_extra_bytes"))], fmt=nbytes, lower_better=True, label_w=110))
c_rows = ["| Server | File before | Live before | File after | Reclaimed | Time | Peak RAM | Peak extra disk |", "|:--|--:|--:|--:|--:|--:|--:|--:|"]
for key, name, _ in SERIES:
    m = SRC[key]["compact"]
    c_rows.append(f"| {name} | {nbytes(m['file_bytes_before'])} | {nbytes(m['active_bytes_before'])} | {nbytes(m['file_bytes_after'])} | "
                  f"{nbytes(m['reclaimed_bytes'])} | {dur(m['secs'])} | {nbytes(get(m, 'resources.rss_peak_bytes'))} | "
                  f"{nbytes(get(m, 'resources.disk_peak_extra_bytes'))} |")
md.append("\n".join(c_rows) + "\n")

md.append("## Disk\n")
md.append(bars("disk-database", "Database file", "after the load, no compaction · lower is better",
               [row("Database", lambda d: get(d, "load.file_bytes_after"))], fmt=nbytes, lower_better=True))
md.append(bars("disk-index", "View index file", "after a fresh build · lower is better",
               [row("View index", lambda d: get(d, "view.index_file_bytes"))], fmt=nbytes, lower_better=True))
md.append(bars("disk-compression", "Compression", "dataset size ÷ database file size after the load · higher is better",
               [row("Compression", lambda d: ds / get(d, "load.file_bytes_after"))], fmt=lambda v: f"{v:.2f}×"))

md.append("## Method\n")
METHOD = [
    "Servers: Apache CouchDB 3.5.3 (official apache/couchdb:3.5.3 image, Erlang/OTP 27, QuickJS) and CouchRS, each on its "
    "own i4i.4xlarge (16 vCPU, 128 GB RAM, data on the instance's local NVMe, XFS). Each server had its own c7i.4xlarge "
    "load generator. The pairs ran at the same time in us-east-1a, in one cluster placement group, alongside two other "
    "CouchRS builds not shown here.",
    "Durability: Linux fdatasync for every server (CouchDB's file:datasync; for CouchRS, RocksDB's WAL sync for documents "
    "and sync_data for view index files).",
    "CouchDB: single node, q=1 (one shard, like CouchRS), automatic compaction (smoosh) off, connection cap raised to "
    "200,000, Erlang port limit 262,144, listen backlog 1,024 (tokio's default, which CouchRS uses). All nodes: 2M open "
    "files, somaxconn 65,535, local ports 1024-65535.",
    "Dataset, phases and the load generator (bench/couchbench) are the same as the Mac runs: 100 GB, deterministic from "
    "a fixed seed, sizes 100 B to 1 GB, generated text.",
    "Replication used a CouchDB replicator: CouchDB's own for CouchDB, and a separate CouchDB container on the CouchRS "
    "server node (port 15984) for CouchRS, copying each database to a new database on the same server.",
    "View queries and the per-size GET/PUT/DELETE tests use the same 16 (or fewer) concurrent clients as on the Mac, so "
    "across a network their rates are bounded by round trips (about 0.5 ms here) as well as by the server; compare the "
    "servers within this report rather than against the Mac numbers.",
    "Server memory and CPU come from a monitor on the server node (resident set and, on Linux, proportional set size; "
    "CPU from /proc), matched to each measured window by wall-clock time.",
    "CouchRS's hybrid engine stores documents in RocksDB and view indexes in its B-tree engine: an append-only index file "
    "with a reduction stored in every node.",
    "View compaction streams each live B-tree, already in key order, into a bottom-up builder, so every node is written once.",
]
md.append("\n".join(f"- {m}" for m in METHOD) + "\n")

with open(dst, "w") as f:
    f.write("\n".join(md))
print(dst, f"({len(os.listdir(chart_dir))} charts in {chart_dir})")
