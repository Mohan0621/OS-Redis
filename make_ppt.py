from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt
import copy

# ── Colours ──────────────────────────────────────────────
RED       = RGBColor(0xE5, 0x39, 0x35)
RED_SOFT  = RGBColor(0xFF, 0x6B, 0x6B)
ORANGE    = RGBColor(0xFF, 0x8A, 0x65)
GOLD      = RGBColor(0xFF, 0xC1, 0x07)
GREEN     = RGBColor(0xA6, 0xE3, 0xA1)
BG        = RGBColor(0x0A, 0x0A, 0x0A)
BG3       = RGBColor(0x1A, 0x1A, 0x1A)
WHITE     = RGBColor(0xFF, 0xFF, 0xFF)
MUTED     = RGBColor(0x88, 0x88, 0x88)
BLUE_L    = RGBColor(0x89, 0xB4, 0xFA)
GREEN_L   = RGBColor(0xA6, 0xE3, 0xA1)
YELLOW_L  = RGBColor(0xF9, 0xE2, 0xAF)
TEAL_L    = RGBColor(0x94, 0xE2, 0xD5)

W = Inches(13.33)   # 16:9 widescreen width
H = Inches(7.5)     # 16:9 widescreen height

prs = Presentation()
prs.slide_width  = W
prs.slide_height = H

blank_layout = prs.slide_layouts[6]  # completely blank

# ── Helpers ──────────────────────────────────────────────

def add_slide():
    sl = prs.slides.add_slide(blank_layout)
    # Dark background
    fill = sl.background.fill
    fill.solid()
    fill.fore_color.rgb = BG
    return sl

def txb(sl, text, x, y, w, h,
        size=20, bold=False, color=WHITE,
        align=PP_ALIGN.LEFT, wrap=True, italic=False):
    tb = sl.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = wrap
    p  = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.size    = Pt(size)
    run.font.bold    = bold
    run.font.italic  = italic
    run.font.color.rgb = color
    return tb

def rect(sl, x, y, w, h, fill_color=BG3, border_color=None, radius=None):
    shape = sl.shapes.add_shape(1, x, y, w, h)  # 1 = rectangle
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    if border_color:
        shape.line.color.rgb = border_color
        shape.line.width = Pt(0.75)
    else:
        shape.line.fill.background()
    return shape

def red_bar(sl, y=Inches(0.08)):
    """Thin red accent bar at top."""
    r = rect(sl, 0, y, W, Inches(0.045), fill_color=RED)
    r.line.fill.background()

def tag_label(sl, text, x=Inches(0.6), y=Inches(0.3)):
    txb(sl, text, x, y, Inches(4), Inches(0.35),
        size=9, color=RED_SOFT, bold=True)

def slide_number(sl, n, total=12):
    txb(sl, f"{n:02d} / {total}", W - Inches(1.5), Inches(0.15),
        Inches(1.4), Inches(0.3), size=9, color=MUTED, align=PP_ALIGN.RIGHT)

def heading(sl, text, x=Inches(0.6), y=Inches(0.75), w=None, size=36):
    if w is None:
        w = W - Inches(1.2)
    txb(sl, text, x, y, w, Inches(0.9), size=size, bold=True, color=WHITE)

def subhead(sl, text, x=Inches(0.6), y=Inches(1.45), w=None, size=15, color=MUTED):
    if w is None:
        w = W - Inches(1.2)
    txb(sl, text, x, y, w, Inches(0.6), size=size, color=color)

def card(sl, x, y, w, h, title, body, ico=None, big_num=None, border_color=None):
    bc = border_color if border_color else RGBColor(0x33, 0x33, 0x33)
    rect(sl, x, y, w, h, fill_color=BG3, border_color=bc)
    cy = y + Inches(0.18)
    if ico:
        txb(sl, ico, x + Inches(0.18), cy, Inches(0.5), Inches(0.4), size=20)
        cy += Inches(0.38)
    if big_num:
        txb(sl, big_num, x + Inches(0.18), cy, w - Inches(0.36), Inches(0.65),
            size=34, bold=True, color=RED_SOFT)
        cy += Inches(0.6)
    if title:
        txb(sl, title, x + Inches(0.18), cy, w - Inches(0.36), Inches(0.35),
            size=13, bold=True, color=WHITE)
        cy += Inches(0.32)
    if body:
        txb(sl, body, x + Inches(0.18), cy, w - Inches(0.36),
            h - (cy - y) - Inches(0.15),
            size=10.5, color=MUTED)

def bullet_list(sl, items, x, y, w, h, check=False, arrow=False, size=11.5):
    tb = sl.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    first = True
    for item in items:
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.space_before = Pt(3)
        prefix = "✓  " if check else ("→  " if arrow else "•  ")
        run = p.add_run()
        run.text = prefix + item
        run.font.size = Pt(size)
        run.font.color.rgb = GREEN if check else (GOLD if arrow else MUTED)

def layer_row(sl, x, y, w, h, badge, title, desc):
    rect(sl, x, y, w, h, fill_color=BG3,
         border_color=RGBColor(0x2A, 0x2A, 0x2A))
    # badge pill
    bp = rect(sl, x + Inches(0.15), y + Inches(0.12),
              Inches(0.85), Inches(0.28), fill_color=RED)
    bp.line.fill.background()
    txb(sl, badge, x + Inches(0.15), y + Inches(0.1),
        Inches(0.85), Inches(0.3), size=8, bold=True,
        color=WHITE, align=PP_ALIGN.CENTER)
    txb(sl, title, x + Inches(1.1), y + Inches(0.08),
        Inches(3.8), Inches(0.3), size=11, bold=True, color=WHITE)
    txb(sl, desc, x + Inches(1.1), y + Inches(0.36),
        w - Inches(1.25), Inches(0.28), size=10, color=MUTED)

# ═══════════════════════════════════════════════════════
# SLIDE 1 — Title
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 1)
tag_label(sl, "OPERATING SYSTEMS PROJECT")

txb(sl, "Redis Clone", Inches(0.6), Inches(1.5), Inches(12), Inches(1.8),
    size=72, bold=True, color=RED_SOFT, align=PP_ALIGN.CENTER)

txb(sl,
    "A Redis-compatible in-memory database server built from scratch in Java\n"
    "RESP Protocol  ·  AOF Persistence  ·  Partitioned Concurrency  ·  51 Commands",
    Inches(0.6), Inches(3.4), Inches(12), Inches(1.2),
    size=16, color=MUTED, align=PP_ALIGN.CENTER)

txb(sl, "Presented to: Operating Systems Faculty   ·   Team: Chaitanya MVR & Mohan",
    Inches(0.6), Inches(5.8), Inches(12), Inches(0.4),
    size=11, color=RGBColor(0x55,0x55,0x55), align=PP_ALIGN.CENTER)

# Red line under title
rect(sl, Inches(5.5), Inches(3.2), Inches(2.3), Inches(0.04),
     fill_color=RED).line.fill.background()

# ═══════════════════════════════════════════════════════
# SLIDE 2 — What is Redis
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 2)
tag_label(sl, "CONTEXT")
heading(sl, "What is Redis?")
subhead(sl,
    "The world's most-used in-memory data store — powering caching, sessions, queues, and "
    "real-time features at Twitter, GitHub, Stack Overflow, and millions of apps.",
    y=Inches(1.42), size=13, color=MUTED)

cw = Inches(3.8); ch = Inches(2.0); cy = Inches(2.4)
card(sl, Inches(0.5),  cy, cw, ch, "In-Memory Speed",
     "All data lives in RAM. Sub-millisecond reads and writes — no disk I/O on the hot path.", ico="⚡")
card(sl, Inches(4.75), cy, cw, ch, "Rich Data Types",
     "Strings, Lists, Hashes, Sets, Sorted Sets — each with its own purpose and command set.", ico="🗂️")
card(sl, Inches(9.0),  cy, cw, ch, "Optional Persistence",
     "AOF logs every write to disk so your data survives crashes and server restarts.", ico="💾")

txb(sl, "Used by: Twitter · GitHub · Stack Overflow · Instagram · Snapchat",
    Inches(0.6), Inches(6.8), Inches(12), Inches(0.35),
    size=10, color=RGBColor(0x44,0x44,0x44), align=PP_ALIGN.CENTER)

# ═══════════════════════════════════════════════════════
# SLIDE 3 — Why we built this
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 3)
tag_label(sl, "MOTIVATION")
heading(sl, "Why Build a Redis Clone?")

cw = Inches(5.8); ch = Inches(2.0)
card(sl, Inches(0.5), Inches(2.0), cw, ch,
     "Concurrency in Practice",
     "Thread pools, blocking queues, semaphores, and synchronization — "
     "implemented from scratch, not just studied in theory.", ico="🧵")
card(sl, Inches(7.0), Inches(2.0), cw, ch,
     "Network I/O",
     "Raw TCP sockets, a custom binary wire protocol (RESP), and "
     "multiple simultaneous client connections.", ico="🔌")
card(sl, Inches(0.5), Inches(4.3), cw, ch,
     "Persistence & File I/O",
     "Append-Only File logging, crash recovery, and log replay — "
     "durability without a database engine.", ico="💽")
card(sl, Inches(7.0), Inches(4.3), cw, ch,
     "OS Primitives Applied",
     "Semaphores, monitors, condition variables, and lock-free partitioning — "
     "the full OS toolbox applied to a real system.", ico="⚙️")

# ═══════════════════════════════════════════════════════
# SLIDE 4 — Architecture
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 4)
tag_label(sl, "ARCHITECTURE")
heading(sl, "System Architecture")
subhead(sl, "Five clean layers from raw TCP bytes to persisted data", y=Inches(1.42), size=13)

rh = Inches(0.75); rx = Inches(0.5); rw = W - Inches(1.0)
ys = [Inches(2.0), Inches(2.85), Inches(3.7), Inches(4.55), Inches(5.4)]
layers = [
    ("LAYER 1", "TCP Server  —  RedisServer.java",
     "Listens on port 6379. Accepts TCP connections. Spawns a RedisClientHandler thread per client."),
    ("LAYER 2", "RESP Protocol  —  RespParser / RespWriter",
     "Decodes the Redis Serialization Protocol from raw bytes into command arrays. Encodes results back."),
    ("LAYER 3", "Command Router  —  CommandRouter.java",
     "Maps each verb to a DataType partition. Enforces concurrency via a shared Semaphore (8 permits, fair)."),
    ("LAYER 4", "Worker Pool  —  DataTypePartition / RedisWorkerPool",
     "12 worker threads across 5 partitions. Each has its own BoundedTaskQueue — zero cross-type contention."),
    ("LAYER 5", "Database + Persistence  —  RedisDatabase / AofLogger",
     "In-memory stores per data type. Every write appended to redis.aof and replayed on startup."),
]
for i, (badge, title, desc) in enumerate(layers):
    layer_row(sl, rx, ys[i], rw, rh, badge, title, desc)

# ═══════════════════════════════════════════════════════
# SLIDE 5 — RESP Protocol
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 5)
tag_label(sl, "PROTOCOL")
heading(sl, "The RESP Protocol")
subhead(sl,
    "Redis Serialization Protocol — a simple line-based format over TCP, human-readable and binary-safe",
    y=Inches(1.42), size=13)

# Left col — 5 wire types
types = [
    ("+", "Simple String", "+OK\\r\\n",           GREEN_L),
    ("-", "Error",         "-ERR message\\r\\n",  RGBColor(0xF3,0x8B,0xA8)),
    (":", "Integer",       ":42\\r\\n",            BLUE_L),
    ("$", "Bulk String",   "$5\\r\\nMohan\\r\\n",  YELLOW_L),
    ("*", "Array",         "*3\\r\\n$3\\r\\nSET\\r\\n...", RGBColor(0xCB,0xA6,0xF7)),
]
ty = Inches(2.1)
for sym, name, ex, col in types:
    rect(sl, Inches(0.5), ty, Inches(5.6), Inches(0.56),
         fill_color=BG3, border_color=RGBColor(0x2A,0x2A,0x2A))
    txb(sl, sym, Inches(0.65), ty + Inches(0.1), Inches(0.35), Inches(0.36),
        size=18, bold=True, color=col)
    txb(sl, name, Inches(1.1), ty + Inches(0.1), Inches(2.0), Inches(0.36),
        size=12, bold=True, color=WHITE)
    txb(sl, ex, Inches(3.2), ty + Inches(0.12), Inches(2.8), Inches(0.32),
        size=10, color=MUTED, italic=True)
    ty += Inches(0.65)

# Right col — wire example
txb(sl, "SET key value — on the wire",
    Inches(6.9), Inches(2.0), Inches(6.0), Inches(0.35),
    size=11, color=MUTED, bold=True)

rect(sl, Inches(6.9), Inches(2.4), Inches(6.0), Inches(4.6),
     fill_color=RGBColor(0x0C,0x0C,0x0C),
     border_color=RGBColor(0x2A,0x2A,0x2A))

wire_lines = [
    ("// Client sends:",    MUTED),
    ("*3",                  TEAL_L),
    ("   ← array of 3 elements", MUTED),
    ("$3",                  TEAL_L),
    ("   ← bulk string, 3 bytes",MUTED),
    ("SET",                 BLUE_L),
    ("$4",                  TEAL_L),
    ("   ← bulk string, 4 bytes",MUTED),
    ("name",                GREEN_L),
    ("$5",                  TEAL_L),
    ("   ← bulk string, 5 bytes",MUTED),
    ("Mohan",               YELLOW_L),
    ("",                    MUTED),
    ("// Server replies:",  MUTED),
    ("+OK",                 TEAL_L),
    ("   ← simple string",  MUTED),
]
# render as one textbox
tb = sl.shapes.add_textbox(Inches(7.1), Inches(2.5), Inches(5.6), Inches(4.2))
tf = tb.text_frame; tf.word_wrap = False
first = True
for line_text, col in wire_lines:
    p = tf.paragraphs[0] if first else tf.add_paragraph()
    first = False
    run = p.add_run()
    run.text = line_text
    run.font.size = Pt(11)
    run.font.color.rgb = col
    run.font.name = "Consolas"

# ═══════════════════════════════════════════════════════
# SLIDE 6 — Concurrency
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 6)
tag_label(sl, "OS CONCEPTS · CONCURRENCY")
heading(sl, "Partitioned Concurrency Model")
subhead(sl,
    "Commands never block each other across data types — each type has its own isolated worker pool",
    y=Inches(1.42), size=13)

# Flow boxes
boxes = [
    ("Client", "TCP", None),
    ("Router", "classify verb", None),
    ("STRING", "4 workers", RED),
    ("LIST",   "2 workers", None),
    ("HASH",   "2 workers", None),
    ("SET",    "2 workers", None),
    ("ZSET",   "2 workers", None),
]
bw = Inches(1.4); bh = Inches(0.75); by = Inches(2.1)
spacing = Inches(1.55)
for i, (title, sub, border) in enumerate(boxes):
    bx = Inches(0.3) + i * spacing
    bc = border if border else RGBColor(0x2A,0x2A,0x2A)
    fc = RGBColor(0x2A,0x06,0x06) if border else BG3
    rect(sl, bx, by, bw, bh, fill_color=fc, border_color=bc)
    txb(sl, title, bx, by + Inches(0.1), bw, Inches(0.3),
        size=12, bold=True, color=WHITE if not border else RED_SOFT,
        align=PP_ALIGN.CENTER)
    txb(sl, sub, bx, by + Inches(0.4), bw, Inches(0.25),
        size=9, color=MUTED, align=PP_ALIGN.CENTER)
    if i < len(boxes) - 1:
        txb(sl, "→", bx + bw, by + Inches(0.2), Inches(0.15), Inches(0.35),
            size=14, color=MUTED, align=PP_ALIGN.CENTER)

# 3 concept cards
cw = Inches(3.9); ch = Inches(2.4); cy = Inches(3.3)
card(sl, Inches(0.5), cy, cw, ch,
     "🔒 Semaphore Gate",
     "Semaphore(8, fair) shared by all partitions. Caps total concurrent DB operations. "
     "Prevents thundering herd under spike load.")
card(sl, Inches(4.7), cy, cw, ch,
     "📦 BoundedTaskQueue",
     "Each worker has a fixed-capacity blocking queue. Full queue throws "
     "TaskRejectedException — error sent to client. No unbounded memory growth.")
card(sl, Inches(8.9), cy, cw, ch,
     "🔑 Key Hashing",
     "Within a partition, tasks are hashed by key. Same key always hits the same worker, "
     "preserving per-key ordering and preventing races.")

# ═══════════════════════════════════════════════════════
# SLIDE 7 — AOF Persistence
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 7)
tag_label(sl, "OS CONCEPTS · PERSISTENCE")
heading(sl, "AOF Persistence")
subhead(sl,
    "Every write command is appended to redis.aof in RESP format before the response is returned",
    y=Inches(1.42), size=13)

# Left bullets
bullet_items = [
    "Write path: Execute command → return result → append to AOF file",
    "Read path: No file access — all reads come directly from RAM",
    "Startup replay: Server reads AOF line by line and re-executes every command",
    "Durability: Even after a crash, the AOF contains the full write history",
    "Lazy expiry: Keys with TTL are checked on access — expired keys silently removed",
]
ty = Inches(2.1)
for item in bullet_items:
    rect(sl, Inches(0.5), ty, Inches(0.07), Inches(0.28),
         fill_color=RED).line.fill.background()
    txb(sl, item, Inches(0.72), ty, Inches(5.8), Inches(0.35),
        size=11.5, color=MUTED)
    ty += Inches(0.52)

# Right — AOF file content
txb(sl, "What redis.aof looks like",
    Inches(7.0), Inches(2.0), Inches(5.9), Inches(0.35),
    size=11, color=MUTED, bold=True)

rect(sl, Inches(7.0), Inches(2.4), Inches(5.9), Inches(2.0),
     fill_color=RGBColor(0x0C,0x0C,0x0C),
     border_color=RGBColor(0x2A,0x2A,0x2A))

aof_lines = [
    "*3\\r\\n$3\\r\\nSET\\r\\n$4\\r\\nname\\r\\n$5\\r\\nMohan\\r\\n",
    "*3\\r\\n$4\\r\\nHSET\\r\\n$6\\r\\nuser:1\\r\\n...",
    "*2\\r\\n$5\\r\\nINCR\\r\\n$7\\r\\ncounter\\r\\n",
    "*4\\r\\n$5\\r\\nLPUSH\\r\\n$6\\r\\nmylist\\r\\n...",
]
tb = sl.shapes.add_textbox(Inches(7.2), Inches(2.5), Inches(5.5), Inches(1.8))
tf = tb.text_frame; tf.word_wrap = False; first = True
for line in aof_lines:
    p = tf.paragraphs[0] if first else tf.add_paragraph()
    first = False
    run = p.add_run(); run.text = line
    run.font.size = Pt(10); run.font.color.rgb = MUTED; run.font.name = "Consolas"

# Crash card
rect(sl, Inches(7.0), Inches(4.55), Inches(5.9), Inches(1.3),
     fill_color=BG3, border_color=RGBColor(0x4A,0x3A,0x10))
txb(sl, "⚠  Crash Recovery",
    Inches(7.2), Inches(4.68), Inches(5.5), Inches(0.35),
    size=12, bold=True, color=GOLD)
txb(sl,
    "If the server crashes mid-write, the last line may be a partial command. "
    "Replay skips it and continues — no data loss beyond the incomplete command.",
    Inches(7.2), Inches(5.05), Inches(5.5), Inches(0.7),
    size=10.5, color=MUTED)

# ═══════════════════════════════════════════════════════
# SLIDE 8 — 51 Commands
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 8)
tag_label(sl, "FEATURES")
heading(sl, "51 Commands Across 6 Categories")

cw = Inches(3.95); ch = Inches(2.05)
cmds = [
    ("19", "String + TTL",
     "SET NX/XX/EX/PX, GET, DEL, INCR, DECR, APPEND, STRLEN, MSET, MGET, GETEX, GETDEL, EXPIRE, PEXPIRE, TTL, PTTL, PERSIST"),
    ("13", "List",
     "LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LREM, LINSERT, LTRIM, BLPOP, BRPOP"),
    ("12", "Hash",
     "HSET, HMSET, HGET, HMGET, HDEL, HGETALL, HKEYS, HVALS, HEXISTS, HLEN, HINCRBY, HINCRBYFLOAT"),
    ("14", "Set",
     "SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER, SDIFF, +STORE variants, SMOVE, SRANDMEMBER, SPOP"),
    ("15", "Sorted Set",
     "ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZINCRBY, ZRANGEBYSCORE, ZCOUNT, ZRANGEBYLEX, ZLEXCOUNT..."),
    (None, "🔁 Blocking Pop",
     "BLPOP/BRPOP block using Java Condition.await() until data arrives or timeout expires."),
]
positions = [
    (Inches(0.4),  Inches(2.0)),
    (Inches(4.65), Inches(2.0)),
    (Inches(8.9),  Inches(2.0)),
    (Inches(0.4),  Inches(4.3)),
    (Inches(4.65), Inches(4.3)),
    (Inches(8.9),  Inches(4.3)),
]
for (num, title, body), (cx, cy) in zip(cmds, positions):
    card(sl, cx, cy, cw, ch, title, body, big_num=num)

# ═══════════════════════════════════════════════════════
# SLIDE 9 — Live Demo
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 9)
tag_label(sl, "DEMO")
heading(sl, "Live Demo")
subhead(sl, "Compile once — start server — connect client — type commands", y=Inches(1.42), size=13)

# Left pane
txb(sl, "Start", Inches(0.5), Inches(2.05), Inches(2), Inches(0.3),
    size=9, color=MUTED, bold=True)
rect(sl, Inches(0.5), Inches(2.4), Inches(5.9), Inches(1.3),
     fill_color=RGBColor(0x0C,0x0C,0x0C), border_color=RGBColor(0x2A,0x2A,0x2A))
tb = sl.shapes.add_textbox(Inches(0.65), Inches(2.5), Inches(5.6), Inches(1.1))
tf = tb.text_frame; tf.word_wrap = False; first = True
start_lines = [
    ("# Compile",                                               MUTED),
    ("javac -d out (Get-Content sources.txt)",                  BLUE_L),
    ("# Terminal 1 - Server",                                   MUTED),
    ("java -cp out com.redis.server.RedisServer",               BLUE_L),
    ("# Terminal 2 - Client",                                   MUTED),
    ("java -cp out com.redis.stress.InteractiveClient",         BLUE_L),
]
for txt, col in start_lines:
    p = tf.paragraphs[0] if first else tf.add_paragraph()
    first = False
    run = p.add_run(); run.text = txt
    run.font.size = Pt(10); run.font.color.rgb = col; run.font.name = "Consolas"

txb(sl, "Strings + TTL", Inches(0.5), Inches(3.85), Inches(3), Inches(0.3),
    size=9, color=MUTED, bold=True)
rect(sl, Inches(0.5), Inches(4.2), Inches(5.9), Inches(1.3),
     fill_color=RGBColor(0x0C,0x0C,0x0C), border_color=RGBColor(0x2A,0x2A,0x2A))
tb2 = sl.shapes.add_textbox(Inches(0.65), Inches(4.3), Inches(5.6), Inches(1.1))
tf2 = tb2.text_frame; tf2.word_wrap = False; first = True
str_lines = [
    ("> SET name Mohan EX 60",   BLUE_L),
    ("OK",                        TEAL_L),
    ("> GET name",               BLUE_L),
    ('"Mohan"',                   YELLOW_L),
    ("> INCR visits",            BLUE_L),
    ("(integer) 1",               TEAL_L),
    ("> TTL name",               BLUE_L),
    ("(integer) 57",              TEAL_L),
]
for txt, col in str_lines:
    p = tf2.paragraphs[0] if first else tf2.add_paragraph()
    first = False
    run = p.add_run(); run.text = txt
    run.font.size = Pt(10); run.font.color.rgb = col; run.font.name = "Consolas"

# Right pane
txb(sl, "Hash — store objects", Inches(6.9), Inches(2.05), Inches(4), Inches(0.3),
    size=9, color=MUTED, bold=True)
rect(sl, Inches(6.9), Inches(2.4), Inches(6.0), Inches(1.55),
     fill_color=RGBColor(0x0C,0x0C,0x0C), border_color=RGBColor(0x2A,0x2A,0x2A))
tb3 = sl.shapes.add_textbox(Inches(7.05), Inches(2.5), Inches(5.7), Inches(1.35))
tf3 = tb3.text_frame; tf3.word_wrap = False; first = True
hash_lines = [
    ("> HSET user:1 name Mohan age 21 city Hyd", BLUE_L),
    ("(integer) 3",                               TEAL_L),
    ("> HGETALL user:1",                          BLUE_L),
    ('1) "name"  2) "Mohan"',                     YELLOW_L),
    ('3) "age"   4) "21"',                         YELLOW_L),
    ('5) "city"  6) "Hyd"',                        YELLOW_L),
]
for txt, col in hash_lines:
    p = tf3.paragraphs[0] if first else tf3.add_paragraph()
    first = False
    run = p.add_run(); run.text = txt
    run.font.size = Pt(10); run.font.color.rgb = col; run.font.name = "Consolas"

txb(sl, "Sorted Set — leaderboard", Inches(6.9), Inches(4.1), Inches(4), Inches(0.3),
    size=9, color=MUTED, bold=True)
rect(sl, Inches(6.9), Inches(4.45), Inches(6.0), Inches(1.55),
     fill_color=RGBColor(0x0C,0x0C,0x0C), border_color=RGBColor(0x2A,0x2A,0x2A))
tb4 = sl.shapes.add_textbox(Inches(7.05), Inches(4.55), Inches(5.7), Inches(1.35))
tf4 = tb4.text_frame; tf4.word_wrap = False; first = True
zset_lines = [
    ("> ZADD scores 300 alice 150 bob 200 charlie", BLUE_L),
    ("(integer) 3",                                  TEAL_L),
    ("> ZREVRANGE scores 0 -1 WITHSCORES",           BLUE_L),
    ('1) "alice"    2) "300.0"',                      YELLOW_L),
    ('3) "charlie"  4) "200.0"',                      YELLOW_L),
    ('5) "bob"      6) "150.0"',                      YELLOW_L),
]
for txt, col in zset_lines:
    p = tf4.paragraphs[0] if first else tf4.add_paragraph()
    first = False
    run = p.add_run(); run.text = txt
    run.font.size = Pt(10); run.font.color.rgb = col; run.font.name = "Consolas"

# ═══════════════════════════════════════════════════════
# SLIDE 10 — Stats
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 10)
tag_label(sl, "BY THE NUMBERS")
heading(sl, "Project Stats")

sw = Inches(2.8); sh = Inches(1.8); sy = Inches(2.0)
stats = [
    ("31", "Java Files",   "All compile cleanly, 0 errors"),
    ("51", "Commands",     "Across 5 Redis data types"),
    ("12", "Workers",      "Across 5 type partitions"),
    ("5",  "Data Types",   "String List Hash Set ZSet"),
]
for i, (num, title, sub) in enumerate(stats):
    sx = Inches(0.5) + i * Inches(3.1)
    card(sl, sx, sy, sw, sh, title, sub, big_num=num)

# Worker partition table
txb(sl, "Worker Thread Distribution by Partition",
    Inches(0.5), Inches(4.1), Inches(8), Inches(0.35),
    size=11, color=MUTED, bold=True)

headers = ["Partition", "Workers", "Queue Capacity", "Primary Commands"]
col_widths = [Inches(2.0), Inches(1.4), Inches(2.0), Inches(5.5)]
hx = Inches(0.5); hy = Inches(4.55)
for j, (hdr, cw2) in enumerate(zip(headers, col_widths)):
    txb(sl, hdr, hx, hy, cw2, Inches(0.3),
        size=10, bold=True, color=RED_SOFT)
    hx += cw2

rows = [
    ("STRING", "4", "16 per worker", "GET SET DEL INCR DECR MSET MGET EXPIRE TTL"),
    ("LIST",   "2", "16 per worker", "LPUSH RPUSH LPOP RPOP LRANGE BLPOP BRPOP"),
    ("HASH",   "2", "16 per worker", "HSET HGET HDEL HGETALL HKEYS HVALS"),
    ("SET",    "2", "16 per worker", "SADD SREM SMEMBERS SUNION SINTER SDIFF"),
    ("ZSET",   "2", "16 per worker", "ZADD ZREM ZRANGE ZSCORE ZRANK ZINCRBY"),
]
row_colors = [RGBColor(0x14,0x14,0x14), BG3]
for ri, row in enumerate(rows):
    ry = Inches(4.9) + ri * Inches(0.37)
    rect(sl, Inches(0.5), ry, Inches(12.4), Inches(0.35),
         fill_color=row_colors[ri % 2]).line.fill.background()
    rx2 = Inches(0.5)
    for val, cw2 in zip(row, col_widths):
        col = RED_SOFT if ri == 0 and val == "STRING" else MUTED
        txb(sl, val, rx2, ry + Inches(0.04), cw2, Inches(0.28), size=10, color=col)
        rx2 += cw2

# ═══════════════════════════════════════════════════════
# SLIDE 11 — Strengths & Next Steps
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 11)
tag_label(sl, "EVALUATION")
heading(sl, "What Works & What's Next")

# Good card
rect(sl, Inches(0.5), Inches(2.0), Inches(5.9), Inches(4.9),
     fill_color=BG3, border_color=RGBColor(0x2A,0x44,0x2A))
txb(sl, "✅  What Works Well",
    Inches(0.7), Inches(2.15), Inches(5.5), Inches(0.4),
    size=14, bold=True, color=GREEN_L)
good = [
    "Full RESP protocol — compatible with any Redis client",
    "AOF persistence with startup replay",
    "Partitioned concurrency — no global lock",
    "TTL / lazy expiry on all keys",
    "Blocking BLPOP using Condition.await()",
    "Pool monitoring with health metrics",
    "51 commands across all 5 data types",
    "Interactive REPL client for live testing",
]
bullet_list(sl, good, Inches(0.7), Inches(2.65), Inches(5.5), Inches(4.0),
            check=True, size=11.5)

# Improve card
rect(sl, Inches(6.9), Inches(2.0), Inches(5.9), Inches(4.9),
     fill_color=BG3, border_color=RGBColor(0x44,0x3A,0x10))
txb(sl, "🔧  Future Improvements",
    Inches(7.1), Inches(2.15), Inches(5.5), Inches(0.4),
    size=14, bold=True, color=GOLD)
improve = [
    "Add Maven or Gradle build file",
    "Fix RespWriter to flush once per full response",
    "AOF truncation recovery on crash restart",
    "Active expiry background thread",
    "MULTI / EXEC transaction support",
    "PUBLISH / SUBSCRIBE pub-sub",
    "AUTH password authentication",
    "JUnit 5 automated test suite",
]
bullet_list(sl, improve, Inches(7.1), Inches(2.65), Inches(5.5), Inches(4.0),
            arrow=True, size=11.5)

# ═══════════════════════════════════════════════════════
# SLIDE 12 — Thank You
# ═══════════════════════════════════════════════════════
sl = add_slide()
red_bar(sl)
slide_number(sl, 12)
tag_label(sl, "THANK YOU")

txb(sl, "Thank You", Inches(0.6), Inches(1.4), Inches(12), Inches(1.6),
    size=68, bold=True, color=RED_SOFT, align=PP_ALIGN.CENTER)

rect(sl, Inches(5.5), Inches(3.1), Inches(2.3), Inches(0.04),
     fill_color=RED).line.fill.background()

txb(sl,
    "We built a working Redis-compatible server from scratch in Java —\n"
    "proving that OS concepts like thread pools, semaphores, blocking queues,\n"
    "and file I/O come together to form real production-grade systems.",
    Inches(1.0), Inches(3.25), Inches(11.3), Inches(1.4),
    size=15, color=MUTED, align=PP_ALIGN.CENTER)

cw = Inches(3.6); ch = Inches(1.5); cy = Inches(5.0)
card(sl, Inches(0.6),  cy, cw, ch, "Source Code",    "d:\\Mohan\\OS\\src\\", ico="💻")
card(sl, Inches(4.85), cy, cw, ch, "Documentation",  "COMMANDS.md & CODEBASE.md at project root", ico="📖")
card(sl, Inches(9.1),  cy, cw, ch, "Questions?",     "Happy to run a live demo or walk through any file", ico="🙋")

# ── Save ──────────────────────────────────────────────
out = r"d:\Mohan\OS\Redis_Clone_Presentation.pptx"
prs.save(out)
print(f"Saved: {out}")
