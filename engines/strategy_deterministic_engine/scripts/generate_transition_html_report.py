from pathlib import Path
import html
import json
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
BASE = REPO_ROOT / "data" / "strategy_transition_reports"
csv_path = BASE / "strategy_transition_scanner_latest.csv"
html_path = BASE / "strategy_transition_report_latest.html"

df = pd.read_csv(csv_path)

if df.empty:
    html_doc = "<h2>Strategy Transition Report</h2><p>No transitions found.</p>"
    html_path.write_text(html_doc)
    print(f"Saved HTML report: {html_path}")
    raise SystemExit(0)

for col in ["price_change_pct", "volume_ratio_20d", "strength_delta", "confidence_delta"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")


def classify_transition(row):
    prev_s = str(row.get("previous_strategy_family", ""))
    curr_s = str(row.get("current_strategy_family", ""))
    prev_r = str(row.get("previous_regime_bucket", ""))
    curr_r = str(row.get("current_regime_bucket", ""))

    if prev_r == "BEARISH" and curr_r == "FLAT":
        return "Bearish weakening"
    if prev_r == "FLAT" and curr_r == "BULLISH":
        return "Bullish strengthening"
    if prev_r == "FLAT" and curr_r == "BEARISH":
        return "Bearish strengthening"
    if prev_r == "BULLISH" and curr_r == "FLAT":
        return "Bullish weakening"
    if prev_s == "BULL_CALL_SPREAD" and curr_s == "BULL_PUT_SPREAD":
        return "Bullish de-risking"
    if prev_s == "IRON_CONDOR" and curr_s == "BULL_PUT_SPREAD":
        return "Bullish shift"
    if prev_s == "IRON_CONDOR" and curr_s == "BEAR_CALL_SPREAD":
        return "Bearish shift"
    change_types = str(row.get("change_types", ""))

    if "TOPN_ENTERED" in change_types:
        return "Entered top-N"

    if "TOPN_EXITED" in change_types:
        return "Exited top-N"

    if "STRENGTH_JUMP" in change_types and "CONFIDENCE_JUMP" in change_types:
        return "Strength + confidence change"

    if "STRENGTH_JUMP" in change_types:
        return "Strength change"

    if "CONFIDENCE_JUMP" in change_types:
        return "Confidence change"

    if "TRANSITION_STATE_CHANGE" in change_types:
        return "Transition-state change"

    return "Minor diagnostic change"


def price_action(v):
    if pd.isna(v):
        return "Unknown"
    if v >= 3:
        return "Strong positive"
    if v >= 1:
        return "Positive"
    if v <= -3:
        return "Strong negative"
    if v <= -1:
        return "Negative"
    return "Neutral"


def volume_action(v):
    if pd.isna(v):
        return "Unknown"
    if v >= 2:
        return "Very high volume"
    if v >= 1.5:
        return "High volume"
    if v >= 1:
        return "Normal volume"
    return "Low volume"


def signal_quality(row):
    ct = str(row.get("change_types", ""))
    vol = row.get("volume_ratio_20d")
    pchg = row.get("price_change_pct")

    structural = ("STRATEGY_FAMILY_CHANGE" in ct) or ("REGIME_BUCKET_CHANGE" in ct)
    topn = "TOPN_ENTERED" in ct
    high_vol = pd.notna(vol) and vol >= 1.5
    strong_price = pd.notna(pchg) and abs(pchg) >= 2

    if topn and high_vol:
        return "Very High"
    if structural and high_vol and strong_price:
        return "High"
    if structural and (high_vol or strong_price):
        return "Medium"
    return "Low"


df["transition_direction"] = df.apply(classify_transition, axis=1)
df["price_action"] = df["price_change_pct"].apply(price_action)
df["volume_action"] = df["volume_ratio_20d"].apply(volume_action)
df["signal_quality"] = df.apply(signal_quality, axis=1)

def build_change_summary(row):
    parts = []

    prev_strategy = str(row.get("previous_strategy_family", ""))
    curr_strategy = str(row.get("current_strategy_family", ""))
    prev_regime = str(row.get("previous_regime_bucket", ""))
    curr_regime = str(row.get("current_regime_bucket", ""))

    if prev_strategy != curr_strategy:
        parts.append(f"Strategy: {prev_strategy} → {curr_strategy}")

    if prev_regime != curr_regime:
        parts.append(f"Regime: {prev_regime} → {curr_regime}")

    if str(row.get("previous_transition_state", "")) != str(row.get("current_transition_state", "")):
        parts.append(
            f"Transition state: {row.get('previous_transition_state', '')} → {row.get('current_transition_state', '')}"
        )

    if bool(row.get("previous_include_in_top_n", False)) != bool(row.get("current_include_in_top_n", False)):
        parts.append(
            "Top-N: entered" if bool(row.get("current_include_in_top_n", False)) else "Top-N: exited"
        )

    if not parts:
        parts.append(str(row.get("change_types", "")))

    return "; ".join(parts)


df["change_summary"] = df.apply(build_change_summary, axis=1)


important = df[
    df["change_types"].fillna("").str.contains(
        "STRATEGY_FAMILY_CHANGE|REGIME_BUCKET_CHANGE|TOPN_ENTERED",
        na=False,
    )
].copy()

high_priority = important[
    important["signal_quality"].isin(["Very High", "High"])
].copy()

run_date = str(df["run_date"].max()) if "run_date" in df.columns else ""

total = len(df)
important_count = len(important)
high_priority_count = len(high_priority)
topn_entered = df["change_types"].fillna("").str.contains("TOPN_ENTERED").sum()
topn_exited = df["change_types"].fillna("").str.contains("TOPN_EXITED").sum()
high_volume_count = (df["volume_ratio_20d"] >= 1.5).sum() if "volume_ratio_20d" in df else 0

category_rows = {}
for category, group in df.groupby("transition_direction"):
    category_rows[category] = group[
        [
            "symbol",
            "close",
            "current_strategy_family",
            "current_regime_bucket",
            "current_strength",
            "current_confidence",
            "change_summary",
            "price_change_pct",
            "volume_ratio_20d",
            "signal_quality",
        ]
    ].fillna("").to_dict("records")

quality_category_rows = {}
for (category, quality), group in df.groupby(["transition_direction", "signal_quality"]):
    key = f"{category}||{quality}"
    quality_category_rows[key] = group[
        [
            "symbol",
            "close",
            "current_strategy_family",
            "current_regime_bucket",
            "current_strength",
            "current_confidence",
            "change_summary",
            "price_change_pct",
            "volume_ratio_20d",
            "signal_quality",
        ]
    ].fillna("").to_dict("records")

category_rows_json = json.dumps(category_rows)
quality_category_rows_json = json.dumps(quality_category_rows)

direction_counts = (
    df["transition_direction"]
    .value_counts()
    .rename_axis("transition_direction")
    .reset_index(name="count")
)

quality_counts = (
    df["signal_quality"]
    .value_counts()
    .rename_axis("signal_quality")
    .reset_index(name="count")
)

QUALITY_ORDER = ["Very High", "High", "Medium", "Low"]

transition_quality_matrix = (
    df.groupby(["transition_direction", "signal_quality"])
    .size()
    .unstack(fill_value=0)
    .reset_index()
)

for q in QUALITY_ORDER:
    if q not in transition_quality_matrix.columns:
        transition_quality_matrix[q] = 0

transition_quality_matrix["total"] = transition_quality_matrix[QUALITY_ORDER].sum(axis=1)
transition_quality_matrix = transition_quality_matrix.sort_values("total", ascending=False)


def render_transition_quality_matrix(matrix_df):
    if matrix_df.empty:
        return "<p class='muted'>No data.</p>"

    rows = []

    for _, r in matrix_df.iterrows():
        transition = str(r["transition_direction"])
        total = int(r["total"])

        rows.append(
            f"""
            <tr>
                <td class="clickable" onclick="openCategoryModal('{html.escape(transition)}')">
                    {html.escape(transition)}
                    <span class="info-icon" title="{html.escape(INFO_TEXT.get(transition, 'Transition category generated by scanner.'))}">ⓘ</span>
                </td>
                <td><span class="badge very-high clickable-badge" onclick="openQualityModal('{html.escape(transition)}', 'Very High')">{int(r.get("Very High", 0))}</span></td>
                <td><span class="badge high clickable-badge" onclick="openQualityModal('{html.escape(transition)}', 'High')">{int(r.get("High", 0))}</span></td>
                <td><span class="badge medium clickable-badge" onclick="openQualityModal('{html.escape(transition)}', 'Medium')">{int(r.get("Medium", 0))}</span></td>
                <td><span class="badge low clickable-badge" onclick="openQualityModal('{html.escape(transition)}', 'Low')">{int(r.get("Low", 0))}</span></td>
                <td><b>{total}</b></td>
            </tr>
            """
        )

    return (
        "<table>"
        "<thead>"
        "<tr>"
        "<th>Transition Type</th>"
        "<th>Very High</th>"
        "<th>High</th>"
        "<th>Medium</th>"
        "<th>Low</th>"
        "<th>Total</th>"
        "</tr>"
        "</thead>"
        "<tbody>"
        + "".join(rows)
        + "</tbody>"
        "</table>"
    )


def badge(text, cls):
    return f'<span class="badge {cls}">{html.escape(str(text))}</span>'


def fmt_num(v, suffix=""):
    if pd.isna(v):
        return "-"
    return f"{v:.2f}{suffix}"


def render_table(data, limit=None):
    if data.empty:
        return "<p class='muted'>No rows.</p>"

    show = data.copy()
    if limit:
        show = show.head(limit)

    cols = [
        "symbol",
        "signal_quality",
        "transition_direction",
        "change_summary",
        "previous_strategy_family",
        "current_strategy_family",
        "previous_regime_bucket",
        "current_regime_bucket",
        "strength_delta",
        "price_change_pct",
        "price_action",
        "volume_ratio_20d",
        "volume_action",
        "reason_codes",
    ]

    cols = [c for c in cols if c in show.columns]

    rows = []
    for _, r in show[cols].iterrows():
        cells = []
        for c in cols:
            val = r[c]
            if c == "signal_quality":
                cls = {
                    "Very High": "very-high",
                    "High": "high",
                    "Medium": "medium",
                    "Low": "low",
                }.get(str(val), "neutral")
                cells.append(f"<td>{badge(val, cls)}</td>")
            elif c == "price_change_pct":
                cls = "positive" if pd.notna(val) and val > 0 else "negative" if pd.notna(val) and val < 0 else "neutral"
                cells.append(f"<td class='{cls}'>{fmt_num(val, '%')}</td>")
            elif c == "volume_ratio_20d":
                cls = "high" if pd.notna(val) and val >= 1.5 else "neutral"
                cells.append(f"<td class='{cls}'>{fmt_num(val, 'x')}</td>")
            elif c == "strength_delta":
                cls = "positive" if pd.notna(val) and val > 0 else "negative" if pd.notna(val) and val < 0 else "neutral"
                cells.append(f"<td class='{cls}'>{fmt_num(val)}</td>")
            else:
                cells.append(f"<td>{html.escape(str(val)) if pd.notna(val) else '-'}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")

    headers = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    return "<div class='table-scroll'><table><thead><tr>" + headers + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>"


INFO_TEXT = {
    "Transition-state change": "Only the transition state changed, e.g. stable_initial to fast_track.",
    "Strength change": "Strategy strength moved materially compared with previous available date.",
    "Confidence change": "Model confidence moved materially compared with previous available date.",
    "Strength + confidence change": "Both strength and confidence changed materially.",
    "Entered top-N": "Symbol newly entered the top-N actionable list.",
    "Exited top-N": "Symbol moved out of the top-N actionable list.",
    "Bullish weakening": "Bullish regime softened, usually moving from bullish to neutral.",
    "Bullish strengthening": "Neutral or weak setup strengthened into bullish.",
    "Bearish weakening": "Bearish regime softened into neutral.",
    "Bearish strengthening": "Neutral or weak setup strengthened into bearish.",
    "Bullish de-risking": "Aggressive bullish structure moved to a more conservative bullish structure.",
}

def render_bar_chart(count_df, label_col, value_col):
    if count_df.empty:
        return "<p class='muted'>No data.</p>"

    max_v = count_df[value_col].max()
    parts = []
    for _, r in count_df.iterrows():
        label = str(r[label_col])
        value = int(r[value_col])
        width = 0 if max_v == 0 else int((value / max_v) * 100)
        info = INFO_TEXT.get(label, "Transition category generated by scanner.")
        parts.append(
            f"""
            <div class="bar-row">
                <div class="bar-label clickable" onclick="openCategoryModal('{html.escape(label)}')">
                    {html.escape(label)}
                    <span class="info-icon" title="{html.escape(info)}">ⓘ</span>
                </div>
                <div class="bar-track">
                    <div class="bar-fill" style="width:{width}%"></div>
                </div>
                <div class="bar-value">{value}</div>
            </div>
            """
        )
    return "".join(parts)


html_doc = f"""
<html>
<head>
<meta charset="utf-8">
<title>Strategy Transition Report</title>
<style>
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    background: #f6f8fb;
    color: #172033;
    padding: 24px;
}}

.header {{
    background: linear-gradient(135deg, #111827, #1f2937);
    color: white;
    padding: 24px;
    border-radius: 18px;
    margin-bottom: 20px;
}}

.header h1 {{
    margin: 0 0 6px 0;
    font-size: 26px;
}}

.header p {{
    margin: 0;
    color: #d1d5db;
}}

.cards {{
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 14px;
    margin-bottom: 22px;
}}

.card {{
    background: white;
    border-radius: 16px;
    padding: 16px;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08);
}}

.card .label {{
    color: #64748b;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .04em;
}}

.card .value {{
    font-size: 26px;
    font-weight: 700;
    margin-top: 8px;
}}

.grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 18px;
    margin-bottom: 22px;
}}

.section {{
    background: white;
    border-radius: 16px;
    padding: 18px;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08);
    margin-bottom: 22px;
}}

.section h2 {{
    margin-top: 0;
    font-size: 18px;
}}

table {{
    border-collapse: collapse;
    width: max-content;
    min-width: 100%;
    font-size: 13px;
}}

.table-scroll {{
    width: 100%;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
}}

th {{
    background: #eef2f7;
    color: #334155;
    text-align: left;
    padding: 9px;
    position: sticky;
    top: 0;
}}

td {{
    border-bottom: 1px solid #e5e7eb;
    padding: 8px;
    vertical-align: top;
}}

tr:hover {{
    background: #f8fafc;
}}

.badge {{
    display: inline-block;
    padding: 4px 9px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 700;
}}

.very-high {{
    background: #fee2e2;
    color: #991b1b;
}}

.high {{
    background: #ffedd5;
    color: #9a3412;
}}

.medium {{
    background: #fef9c3;
    color: #854d0e;
}}

.low {{
    background: #e0f2fe;
    color: #075985;
}}

.positive {{
    color: #047857;
    font-weight: 700;
}}

.negative {{
    color: #b91c1c;
    font-weight: 700;
}}

.neutral {{
    color: #475569;
}}

.muted {{
    color: #64748b;
}}

.bar-row {{
    display: grid;
    grid-template-columns: 180px 1fr 40px;
    gap: 10px;
    align-items: center;
    margin: 9px 0;
}}

.bar-label {{
    font-size: 13px;
    color: #334155;
}}

.info-icon {{
    display: inline-block;
    margin-left: 6px;
    color: #64748b;
    font-size: 12px;
    cursor: help;
}}

.clickable {{
    cursor: pointer;
    text-decoration: underline dotted;
}}

.clickable-badge {{
    cursor: pointer;
}}

.modal {{
    display: none;
    position: fixed;
    z-index: 9999;
    left: 0;
    top: 0;
    width: 100%;
    height: 100%;
    overflow: auto;
    background: rgba(15, 23, 42, 0.55);
}}

.modal-content {{
    background: white;
    margin: 5% auto;
    padding: 22px;
    border-radius: 18px;
    width: 88%;
    max-height: 80vh;
    overflow: auto;
    box-shadow: 0 20px 60px rgba(15, 23, 42, 0.35);
}}

.close {{
    float: right;
    font-size: 28px;
    font-weight: 700;
    cursor: pointer;
}}

.bar-track {{
    height: 12px;
    background: #e5e7eb;
    border-radius: 999px;
    overflow: hidden;
}}

.bar-fill {{
    height: 100%;
    background: #334155;
    border-radius: 999px;
}}

.bar-value {{
    text-align: right;
    font-weight: 700;
}}


.search-box {
    width: 100%;
    padding: 12px 14px;
    border: 1px solid #d0d5dd;
    border-radius: 10px;
    font-size: 14px;
    box-sizing: border-box;
}

.footer {{
    color: #64748b;

    font-size: 12px;
    margin-top: 20px;
}}
</style>
</head>

<body>

<div class="header">
    <h1>Strategy Transition Report</h1>
    <p>Run date: {html.escape(run_date)} · Deterministic strategy transition scanner with price and volume confirmation</p>
</div>


<div class="section">
    <h2>Search</h2>
    <input
        id="globalSearch"
        class="search-box"
        type="text"
        placeholder="Search symbol, strategy, transition, reason code, signal quality...">
</div>


<div class="cards">
    <div class="card"><div class="label">Total Changes</div><div class="value">{total}</div></div>
    <div class="card"><div class="label">Important</div><div class="value">{important_count}</div></div>
    <div class="card"><div class="label">High Priority</div><div class="value">{high_priority_count}</div></div>
    <div class="card"><div class="label">Top-N Entered</div><div class="value">{topn_entered}</div></div>
    <div class="card"><div class="label">High Volume</div><div class="value">{high_volume_count}</div></div>
</div>

<div class="section">
    <h2>Transition Quality Matrix</h2>
    <p class="muted">
        Combines transition type with signal quality. Click any transition type to inspect symbols.
    </p>
    {render_transition_quality_matrix(transition_quality_matrix)}
</div>

<div class="section">
    <h2>High-Priority Transitions</h2>
    <p class="muted">Structural changes with strong price or volume confirmation.</p>
    {render_table(high_priority)}
</div>

<div class="section">
    <h2>All Important Structural Transitions</h2>
    {render_table(important)}
</div>

<div class="section">
    <h2>All Detected Transitions</h2>
    {render_table(df)}
</div>

<div id="categoryModal" class="modal">
    <div class="modal-content">
        <span class="close" onclick="closeCategoryModal()">&times;</span>
        <h2 id="modalTitle"></h2>
        <div id="modalBody"></div>
    </div>
</div>

<div class="footer">
    CSV source: {html.escape(str(csv_path))}<br>
    This report is a decision-support scanner, not an automated trade instruction.
</div>

<script>
const CATEGORY_ROWS = {category_rows_json};
const QUALITY_CATEGORY_ROWS = {quality_category_rows_json};

function openCategoryModal(category) {{
    const rows = CATEGORY_ROWS[category] || [];
    document.getElementById("modalTitle").innerText = category + " (" + rows.length + ")";

    if (rows.length === 0) {{
        document.getElementById("modalBody").innerHTML = "<p>No symbols.</p>";
    }} else {{
        let html = "<table><thead><tr>";
        const cols = Object.keys(rows[0]);
        cols.forEach(c => html += "<th>" + c + "</th>");
        html += "</tr></thead><tbody>";

        rows.forEach(r => {{
            html += "<tr>";
            cols.forEach(c => html += "<td>" + (r[c] ?? "") + "</td>");
            html += "</tr>";
        }});

        html += "</tbody></table>";
        document.getElementById("modalBody").innerHTML = html;
    }}

    document.getElementById("categoryModal").style.display = "block";
}}

function openQualityModal(category, quality) {{
    const key = category + "||" + quality;
    const rows = QUALITY_CATEGORY_ROWS[key] || [];

    document.getElementById("modalTitle").innerText =
        category + " / " + quality + " (" + rows.length + ")";

    if (rows.length === 0) {{
        document.getElementById("modalBody").innerHTML = "<p>No symbols.</p>";
    }} else {{
        let html = "<table><thead><tr>";
        const cols = Object.keys(rows[0]);

        cols.forEach(c => html += "<th>" + c + "</th>");
        html += "</tr></thead><tbody>";

        rows.forEach(r => {{
            html += "<tr>";
            cols.forEach(c => html += "<td>" + (r[c] ?? "") + "</td>");
            html += "</tr>";
        }});

        html += "</tbody></table>";
        document.getElementById("modalBody").innerHTML = html;
    }}

    document.getElementById("categoryModal").style.display = "block";
}}

function closeCategoryModal() {{
    document.getElementById("categoryModal").style.display = "none";
}}

window.onclick = function(event) {{
    const modal = document.getElementById("categoryModal");
    if (event.target === modal) {{
        closeCategoryModal();
    }}
}}

</script>

</body>
</html>
"""

html_path.write_text(html_doc)
print(f"Saved production-grade HTML report: {html_path}")
