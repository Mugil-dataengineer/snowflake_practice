"""
ShopSphere Snowflake Data Warehouse – Architecture Diagram
Generates architecture.png using only Pillow (no matplotlib needed).
"""

from PIL import Image, ImageDraw, ImageFont
import os

# ── Canvas ──────────────────────────────────────────────────────────────────
W, H = 1600, 1000
img = Image.new("RGB", (W, H), "#f0f4f8")
d = ImageDraw.Draw(img)

# ── Fonts (fall back gracefully) ─────────────────────────────────────────────
def load_font(size, bold=False):
    candidates_bold   = ["arialbd.ttf", "Arial_Bold.ttf", "DejaVuSans-Bold.ttf"]
    candidates_normal = ["arial.ttf",   "Arial.ttf",      "DejaVuSans.ttf"]
    for name in (candidates_bold if bold else candidates_normal):
        for root in [r"C:\Windows\Fonts", "/usr/share/fonts/truetype/dejavu", "/usr/share/fonts"]:
            path = os.path.join(root, name)
            if os.path.exists(path):
                try:
                    return ImageFont.truetype(path, size)
                except Exception:
                    pass
    return ImageFont.load_default()

f_title  = load_font(26, bold=True)
f_head   = load_font(15, bold=True)
f_body   = load_font(13)
f_small  = load_font(11)
f_arrow  = load_font(11)

# ── Helpers ──────────────────────────────────────────────────────────────────
def rect(x1, y1, x2, y2, fill, outline, radius=10):
    d.rounded_rectangle([x1, y1, x2, y2], radius=radius, fill=fill, outline=outline, width=2)

def text_centered(x, y, txt, font, color="#1f2328"):
    bb = d.textbbox((0, 0), txt, font=font)
    tw = bb[2] - bb[0]
    d.text((x - tw // 2, y), txt, font=font, fill=color)

def text_block(cx, top, lines, font, color="#1f2328", line_h=18):
    for i, line in enumerate(lines):
        text_centered(cx, top + i * line_h, line, font, color)

def arrow_h(x1, x2, y, color="#555", label=""):
    mid_x = (x1 + x2) // 2
    d.line([(x1, y), (x2, y)], fill=color, width=2)
    # arrowhead
    if x2 > x1:
        d.polygon([(x2, y), (x2-10, y-5), (x2-10, y+5)], fill=color)
    else:
        d.polygon([(x2, y), (x2+10, y-5), (x2+10, y+5)], fill=color)
    if label:
        bb = d.textbbox((0,0), label, font=f_arrow)
        tw = bb[2]-bb[0]
        d.text((mid_x - tw//2, y - 16), label, font=f_arrow, fill=color)

def arrow_v(x, y1, y2, color="#555", label=""):
    mid_y = (y1 + y2) // 2
    d.line([(x, y1), (x, y2)], fill=color, width=2)
    if y2 > y1:
        d.polygon([(x, y2), (x-5, y2-10), (x+5, y2-10)], fill=color)
    else:
        d.polygon([(x, y2), (x-5, y2+10), (x+5, y2+10)], fill=color)
    if label:
        bb = d.textbbox((0,0), label, font=f_arrow)
        tw = bb[2]-bb[0]
        d.text((x + 8, mid_y - 7), label, font=f_arrow, fill=color)

# ════════════════════════════════════════════════════════════════════════════
# TITLE
# ════════════════════════════════════════════════════════════════════════════
d.rectangle([0, 0, W, 54], fill="#1a3a5c")
text_centered(W//2, 12, "ShopSphere – Snowflake Cloud Data Warehouse Architecture", f_title, "#ffffff")

# ════════════════════════════════════════════════════════════════════════════
# LAYER LABELS (left margin)
# ════════════════════════════════════════════════════════════════════════════
layer_labels = [
    (70,  "SOURCE\nSYSTEMS"),
    (250, "INGESTION\nLAYER"),
    (430, "RAW\nLAYER"),
    (590, "TRANSFORM\nLAYER"),
    (750, "ANALYTICS\nLAYER"),
    (880, "ACCESS /\nSECURITY"),
]
for ly, lbl in layer_labels:
    for i, part in enumerate(lbl.split("\n")):
        d.text((10, ly + i*14), part, font=f_small, fill="#57606a")

# ════════════════════════════════════════════════════════════════════════════
# 1. SOURCE SYSTEMS  (y ≈ 70)
# ════════════════════════════════════════════════════════════════════════════
src_y1, src_y2 = 68, 200
src_configs = [
    (110, 290, "#e8f4fd", "#2980b9", "Customer\nManagement\nSystem",    ["Customer ID", "Name / Email", "Location", "Status"]),
    (320, 530, "#e8f4fd", "#2980b9", "Order\nManagement\nSystem",       ["Order ID", "Product ID", "Qty / Price", "Order Status"]),
    (560, 750, "#e8f4fd", "#2980b9", "Payment\nSystem",                  ["Payment ID", "Method", "Amount", "Refund Info"]),
    (780, 990, "#e8f4fd", "#2980b9", "Website\nActivity\nSystem",       ["Session ID", "Views/Search", "Cart Events", "Timestamps"]),
    (1020,1220,"#e8f4fd", "#2980b9", "Product\nCatalog",                 ["Product ID", "Category", "Description", "Price"]),
]

src_centers = []
for x1, x2, fill, outline, title, details in src_configs:
    rect(x1, src_y1, x2, src_y2, fill, outline)
    cx = (x1 + x2) // 2
    src_centers.append((cx, src_y2))
    # title
    for i, part in enumerate(title.split("\n")):
        text_centered(cx, src_y1 + 6 + i*16, part, f_head, "#1a3a5c")
    # details
    for i, det in enumerate(details):
        text_centered(cx, src_y1 + 70 + i*14, "• " + det, f_small, "#555")

# ════════════════════════════════════════════════════════════════════════════
# 2. INGESTION LAYER  (y ≈ 230)
# ════════════════════════════════════════════════════════════════════════════
ing_y1, ing_y2 = 228, 310
ing_configs = [
    (110, 530, "#fff8e1", "#f39c12", "Batch Ingestion",     ["Snowpipe / COPY INTO", "S3 / Azure Blob / GCS Stage", "Scheduled Tasks"]),
    (560, 990, "#fff8e1", "#f39c12", "Streaming Ingestion", ["Snowpipe Streaming", "Kafka Connector", "Website Events (near-real-time)"]),
    (1020,1220,"#fff8e1", "#f39c12", "Flat File / API",     ["CSV / JSON Loads", "External Stage", "Manual / Scheduled"]),
]

ing_centers = []
for x1, x2, fill, outline, title, details in ing_configs:
    rect(x1, ing_y1, x2, ing_y2, fill, outline)
    cx = (x1 + x2) // 2
    ing_centers.append(cx)
    text_centered(cx, ing_y1 + 6, title, f_head, "#7a4a00")
    for i, det in enumerate(details):
        text_centered(cx, ing_y1 + 28 + i*16, det, f_small, "#555")

# Arrows source → ingestion
arrow_pairs_si = [(200, 320), (425, 320), (655, 655), (885, 775), (1120, 1120)]
for (sx, ix) in [(200, 320), (425, 320)]:
    arrow_v(sx, src_y2, ing_y1, "#2980b9")
arrow_v(655, src_y2, ing_y1, "#2980b9")
arrow_v(885, src_y2, ing_y1, "#2980b9")
arrow_v(1120, src_y2, ing_y1, "#2980b9")

# ════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE BOUNDARY BOX
# ════════════════════════════════════════════════════════════════════════════
sf_x1, sf_x2 = 90, 1560
sf_y1, sf_y2 = 318, 870
d.rounded_rectangle([sf_x1, sf_y1, sf_x2, sf_y2], radius=16,
                    fill="#f7fbff", outline="#29b5e8", width=3)
text_centered((sf_x1+sf_x2)//2, sf_y1 + 6, "❄  SNOWFLAKE DATA WAREHOUSE", f_head, "#29b5e8")

# ════════════════════════════════════════════════════════════════════════════
# 3. RAW LAYER  (y ≈ 345)
# ════════════════════════════════════════════════════════════════════════════
raw_y1, raw_y2 = 345, 430
raw_configs = [
    (110, 380, "#e8f8f5", "#27ae60", "RAW_CUSTOMERS",  ["As-is from source", "No transforms"]),
    (395, 640, "#e8f8f5", "#27ae60", "RAW_ORDERS",     ["As-is from source", "No transforms"]),
    (655, 880, "#e8f8f5", "#27ae60", "RAW_PAYMENTS",   ["As-is from source", "No transforms"]),
    (895, 1150,"#e8f8f5", "#27ae60", "RAW_EVENTS",     ["JSON semi-struct.", "VARIANT column"]),
    (1165,1390,"#e8f8f5", "#27ae60", "RAW_PRODUCTS",   ["As-is from source", "No transforms"]),
]

raw_centers = []
for x1, x2, fill, outline, title, details in raw_configs:
    rect(x1, raw_y1, x2, raw_y2, fill, outline)
    cx = (x1+x2)//2
    raw_centers.append(cx)
    text_centered(cx, raw_y1+8, title, f_head, "#1a5c3a")
    for i, det in enumerate(details):
        text_centered(cx, raw_y1+32+i*18, det, f_small, "#555")

# Arrows ingestion → raw
for cx in [320, 775, 1120]:
    arrow_v(cx, ing_y2, raw_y1, "#f39c12")

# ════════════════════════════════════════════════════════════════════════════
# 4. TRANSFORM / STAGING LAYER  (y ≈ 460)
# ════════════════════════════════════════════════════════════════════════════
stg_y1, stg_y2 = 458, 560
stg_configs = [
    (110, 380, "#fef9e7", "#d4ac0d", "STG_CUSTOMERS",   ["Dedup", "SCD Type 2", "Validate"]),
    (395, 640, "#fef9e7", "#d4ac0d", "STG_ORDERS",      ["Dedup", "Normalize", "Status flags"]),
    (655, 880, "#fef9e7", "#d4ac0d", "STG_PAYMENTS",    ["Dedup", "Refund calc.", "Validate"]),
    (895, 1150,"#fef9e7", "#d4ac0d", "STG_EVENTS",      ["Flatten JSON", "Session parse", "Dedup"]),
    (1165,1390,"#fef9e7", "#d4ac0d", "STG_PRODUCTS",    ["Enrich", "Normalize", "Validate"]),
]

stg_centers = []
for x1, x2, fill, outline, title, details in stg_configs:
    rect(x1, stg_y1, x2, stg_y2, fill, outline)
    cx = (x1+x2)//2
    stg_centers.append(cx)
    text_centered(cx, stg_y1+8, title, f_head, "#7a6000")
    for i, det in enumerate(details):
        text_centered(cx, stg_y1+32+i*18, det, f_small, "#555")
    arrow_v(cx, raw_y2, stg_y1, "#27ae60")

# ════════════════════════════════════════════════════════════════════════════
# 5. ANALYTICS LAYER – Star Schema  (y ≈ 590)
# ════════════════════════════════════════════════════════════════════════════
ana_y1, ana_y2 = 588, 710

# Fact table (center)
fact_cx = 750
rect(640, ana_y1, 860, ana_y2, "#fde8e8", "#c0392b")
text_centered(fact_cx, ana_y1+8,  "FACT_ORDERS", f_head, "#7a0000")
for i, t in enumerate(["order_key (PK)", "customer_key (FK)", "product_key (FK)",
                        "payment_key (FK)", "date_key (FK)", "quantity, amount, discount"]):
    text_centered(fact_cx, ana_y1+30+i*16, t, f_small, "#555")

# Dimension tables
dims = [
    (110, 360, "DIM_CUSTOMERS",  ["customer_key", "name, email", "city/state", "SCD Type 2"]),
    (375, 610, "DIM_PRODUCTS",   ["product_key",  "category",   "description","price"]),
    (875, 1090,"DIM_DATE",       ["date_key",     "year/month", "quarter",    "fiscal period"]),
    (1105,1340,"DIM_PAYMENTS",   ["payment_key",  "method",     "status",     "refund_flag"]),
]

dim_centers = []
for x1, x2, fill_c, title, details in [(d[0], d[1], "#fde8e8", d[2], d[3]) for d in dims]:
    rect(x1, ana_y1, x2, ana_y2, "#fdf0e8", "#e67e22")
    cx = (x1+x2)//2
    dim_centers.append(cx)
    text_centered(cx, ana_y1+8, title, f_head, "#7a3a00")
    for i, det in enumerate(details):
        text_centered(cx, ana_y1+30+i*16, det, f_small, "#555")

# Fact ← → Dims  arrows
for dcx in dim_centers:
    if dcx < fact_cx:
        arrow_h(dcx + (610-dcx if dcx==375 else 360-dcx), 640, (ana_y1+ana_y2)//2, "#c0392b")
    else:
        arrow_h(fact_cx+110 if dcx > fact_cx else fact_cx-110, dcx, (ana_y1+ana_y2)//2, "#c0392b")

# Staging → Analytics arrows (just draw from stg centers to fact table midpoints)
for scx in stg_centers[:3]:
    arrow_v(scx, stg_y2, ana_y1, "#d4ac0d")

# ════════════════════════════════════════════════════════════════════════════
# 6. ACCESS / SECURITY  (y ≈ 730)
# ════════════════════════════════════════════════════════════════════════════
acc_y1, acc_y2 = 728, 860
acc_configs = [
    (110,  400, "#f0e6ff", "#8e44ad", "RBAC / Row-Level Security",
        ["Roles: analyst, finance, marketing, admin", "Row-level filters per region/team",
         "Column masking for PII (email, name)"]),
    (415,  760, "#f0e6ff", "#8e44ad", "Virtual Warehouses",
        ["XS warehouse – ad-hoc queries", "M  warehouse  – ETL pipelines",
         "Auto-suspend / Auto-resume"]),
    (775, 1100, "#f0e6ff", "#8e44ad", "Data Quality & Monitoring",
        ["Snowflake Tasks + Streams for CDC", "INFORMATION_SCHEMA quality checks",
         "Alert on pipeline failure"]),
    (1115,1390, "#f0e6ff", "#8e44ad", "BI / Reporting Tools",
        ["Tableau / Power BI / Preset", "Direct Snowflake connector",
         "Secure Views for external sharing"]),
]

for x1, x2, fill, outline, title, details in acc_configs:
    rect(x1, acc_y1, x2, acc_y2, fill, outline)
    cx = (x1+x2)//2
    text_centered(cx, acc_y1+8, title, f_head, "#4a0080")
    for i, det in enumerate(details):
        text_centered(cx, acc_y1+34+i*20, det, f_small, "#333")

arrow_v(fact_cx, ana_y2, acc_y1, "#c0392b", "analytics\nready")

# ════════════════════════════════════════════════════════════════════════════
# FOOTER
# ════════════════════════════════════════════════════════════════════════════
d.rectangle([0, H-30, W, H], fill="#1a3a5c")
text_centered(W//2, H-22, "ShopSphere Data Warehouse  •  Snowflake Architecture  •  ShopSphere Analytics Team", f_small, "#aac")

# ── Save ────────────────────────────────────────────────────────────────────
out_path = "architecture.png"
img.save(out_path, "PNG")
print(f"Saved: {out_path}  ({W}x{H}px)")
