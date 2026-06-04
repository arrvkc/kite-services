from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

REPORT_CSV = "data/strategy_validation/directional_accuracy_report.csv"
VALIDATION_CSV = "data/strategy_validation/fo_universe_validation.csv"
OUTPUT_HTML = "data/strategy_validation/validation_dashboard_split.html"


def main():
    report_df = pd.read_csv(REPORT_CSV)
    signal_df = pd.read_csv(VALIDATION_CSV)

    signal_df["strength_bucket"] = pd.cut(
        signal_df["strength"],
        bins=[-1, 19, 39, 59, 79, 100],
        labels=["00-19", "20-39", "40-59", "60-79", "80-100"],
    )

    report_json = json.dumps(report_df.fillna("").to_dict(orient="records"))
    signal_json = json.dumps(signal_df.fillna("").to_dict(orient="records"))

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Deriviq Research Dashboard Split</title>
<style>
body{{font-family:Arial,sans-serif;margin:0;background:#f8fafc;color:#0f172a}}
.header{{background:#0f172a;color:white;padding:18px 22px}}
.header h1{{margin:0;font-size:22px}}
.toolbar{{display:flex;gap:10px;padding:14px 22px;background:white;border-bottom:1px solid #e5e7eb}}
select,input{{padding:9px 10px;border:1px solid #cbd5e1;border-radius:8px;background:white}}
.layout{{display:grid;grid-template-columns:58% 42%;gap:16px;padding:16px 22px}}
.panel{{background:white;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
.panel-header{{padding:14px 16px;border-bottom:1px solid #e5e7eb;background:#f8fafc;font-weight:bold}}
.table-wrap{{max-height:78vh;overflow:auto}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th,td{{border-bottom:1px solid #e5e7eb;padding:8px;text-align:right;white-space:nowrap}}
th{{background:#f1f5f9;position:sticky;top:0;z-index:2}}
td:first-child,th:first-child,td:nth-child(2),th:nth-child(2),td:nth-child(3),th:nth-child(3),td:nth-child(4),th:nth-child(4),td:nth-child(5),th:nth-child(5){{text-align:left}}
tr:hover{{background:#eff6ff}}
tr.selected{{background:#dbeafe}}
.clickable{{cursor:pointer}}
.green{{background:#16a34a;color:white;font-weight:bold}}
.lgreen{{background:#65a30d;color:white;font-weight:bold}}
.yellow{{background:#ca8a04;color:white;font-weight:bold}}
.red{{background:#dc2626;color:white;font-weight:bold}}
.detail{{padding:16px;max-height:84vh;overflow:auto}}
.detail-title{{font-size:18px;font-weight:bold;margin-bottom:4px}}
.detail-subtitle{{color:#64748b;margin-bottom:16px}}
.kpis{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px}}
.kpi{{background:#f8fafc;border:1px solid #e5e7eb;border-radius:10px;padding:12px}}
.kpi-label{{color:#64748b;font-size:12px}}
.kpi-value{{font-size:22px;font-weight:bold;margin-top:4px}}
.warning{{background:#fef3c7;border:1px solid #f59e0b;color:#92400e;padding:10px;border-radius:8px;margin-bottom:14px}}
.positive{{color:#15803d;font-weight:bold}}
.negative{{color:#b91c1c;font-weight:bold}}
.empty{{padding:24px;color:#64748b}}
</style>
</head>
<body>

<div class="header"><h1>Deriviq Research Dashboard — Split View</h1></div>

<div class="toolbar">
<input id="searchBox" placeholder="Search symbol / strategy..." />
<select id="symbolFilter"><option value="">All Symbols</option></select>
<select id="regimeFilter"><option value="">All Regimes</option></select>
<select id="strategyFilter"><option value="">All Strategies</option></select>
</div>

<div class="layout">
<div class="panel">
<div class="panel-header">Directional Accuracy Rows: <span id="rowCount">0</span></div>
<div class="table-wrap">
<table>
<thead>
<tr>
<th>Scope</th><th>Symbol</th><th>Regime</th><th>Strategy</th><th>Bucket</th>
<th>Signals</th><th>0%</th><th>1%</th><th>2%</th><th>3%</th>
</tr>
</thead>
<tbody id="tbody"></tbody>
</table>
</div>
</div>

<div class="panel">
<div class="panel-header">Signal Drilldown</div>
<div id="detailPanel" class="detail">
<div class="empty">Click any row on the left to see the underlying signal dates.</div>
</div>
</div>
</div>

<script>
const DATA = {report_json};
const SIGNALS = {signal_json};

function heat(v) {{
    if (v >= 60) return 'green';
    if (v >= 50) return 'lgreen';
    if (v >= 40) return 'yellow';
    return 'red';
}}

function fmt(v) {{
    const n = Number(v);
    if (Number.isNaN(n)) return '';
    return n.toFixed(1);
}}

function fmt2(v) {{
    const n = Number(v);
    if (Number.isNaN(n)) return '';
    return n.toFixed(2);
}}

const symbols = [...new Set(DATA.map(x => x.symbol))].sort();
const regimes = [...new Set(DATA.map(x => x.regime))].sort();
const strategies = [...new Set(DATA.map(x => x.strategy_family))].sort();

symbols.forEach(v => symbolFilter.innerHTML += `<option>${{v}}</option>`);
regimes.forEach(v => regimeFilter.innerHTML += `<option>${{v}}</option>`);
strategies.forEach(v => strategyFilter.innerHTML += `<option>${{v}}</option>`);

let selectedKey = '';

function isBullish(strategy) {{
    return strategy === 'BULL_CALL_SPREAD' || strategy === 'BULL_PUT_SPREAD';
}}

function isBearish(strategy) {{
    return strategy === 'BEAR_CALL_SPREAD' || strategy === 'BEAR_PUT_SPREAD';
}}

function isWin(row, threshold) {{
    const r = Number(row.return_5d_pct);
    if (isBullish(row.strategy_family)) return r > threshold;
    if (isBearish(row.strategy_family)) return r < -threshold;
    if (row.strategy_family === 'IRON_CONDOR') return Math.abs(r) <= 3.0;
    return false;
}}

function showDetails(symbol, regime, strategy, bucket) {{
    const rows = SIGNALS.filter(r =>
        (symbol === 'ALL' || r.symbol === symbol) &&
        r.regime === regime &&
        r.strategy_family === strategy &&
        r.strength_bucket === bucket &&
        r.return_5d_pct !== ''
    );

    selectedKey = `${{symbol}}|${{regime}}|${{strategy}}|${{bucket}}`;

    const wins0 = rows.filter(r => isWin(r, 0)).length;
    const wins2 = rows.filter(r => isWin(r, 2)).length;
    const wins3 = rows.filter(r => isWin(r, 3)).length;

    const returns = rows.map(r => Number(r.return_5d_pct));
    const avgReturn = returns.length ? returns.reduce((a,b) => a + b, 0) / returns.length : 0;
    const sortedReturns = [...returns].sort((a,b) => a - b);
    const medianReturn = sortedReturns.length ? sortedReturns[Math.floor(sortedReturns.length / 2)] : 0;

    const latest = [...rows].sort((a,b) => String(b.run_date).localeCompare(String(a.run_date)))[0] || {{}};

    let html = `
        <div class="detail-title">${{symbol}} | ${{regime}} | ${{strategy}} | ${{bucket}}</div>
        <div class="detail-subtitle">Underlying signals behind the selected aggregate row</div>

        <div class="kpis">
            <div class="kpi"><div class="kpi-label">Signals</div><div class="kpi-value">${{rows.length}}</div></div>
            <div class="kpi"><div class="kpi-label">0% Win</div><div class="kpi-value">${{fmt(wins0 * 100 / Math.max(rows.length, 1))}}%</div></div>
            <div class="kpi"><div class="kpi-label">2% Win</div><div class="kpi-value">${{fmt(wins2 * 100 / Math.max(rows.length, 1))}}%</div></div>
            <div class="kpi"><div class="kpi-label">Latest Signal</div><div class="kpi-value">${{latest.run_date || '-'}}</div></div>
            <div class="kpi"><div class="kpi-label">Signal Close</div><div class="kpi-value">${{fmt2(latest.close)}}</div></div>
            <div class="kpi"><div class="kpi-label">Close +5D</div><div class="kpi-value">${{fmt2(latest.close_5d)}}</div></div>
            <div class="kpi"><div class="kpi-label">Latest Return</div><div class="kpi-value">${{fmt2(latest.return_5d_pct)}}%</div></div>
            <div class="kpi"><div class="kpi-label">Avg Return</div><div class="kpi-value">${{fmt2(avgReturn)}}%</div></div>
            <div class="kpi"><div class="kpi-label">Median Return</div><div class="kpi-value">${{fmt2(medianReturn)}}%</div></div>
        </div>
    `;

    if (rows.length < 10) {{
        html += `<div class="warning">⚠ Low sample size: ${{rows.length}} signals. Treat this row as directional evidence, not a conclusion.</div>`;
    }}

    html += `
        <table>
        <thead>
        <tr>
        <th>Date</th><th>Signal Close</th><th>Close +5D</th><th>Strength</th>
        <th>Confidence</th><th>Score</th><th>Return 5D</th><th>0%</th><th>1%</th><th>2%</th><th>3%</th>
        </tr>
        </thead>
        <tbody>
    `;

    rows.sort((a,b) => String(b.run_date).localeCompare(String(a.run_date))).forEach(r => {{
        const ret = Number(r.return_5d_pct);
        const retClass = ret >= 0 ? 'positive' : 'negative';

        html += `
            <tr>
            <td>${{r.run_date}}</td>
            <td>${{fmt2(r.close)}}</td>
            <td>${{fmt2(r.close_5d)}}</td>
            <td>${{r.strength}}</td>
            <td>${{fmt2(r.confidence)}}</td>
            <td>${{fmt2(r.score)}}</td>
            <td class="${{retClass}}">${{fmt2(ret)}}%</td>
            <td>${{isWin(r, 0) ? '✓' : '✗'}}</td>
            <td>${{isWin(r, 1) ? '✓' : '✗'}}</td>
            <td>${{isWin(r, 2) ? '✓' : '✗'}}</td>
            <td>${{isWin(r, 3) ? '✓' : '✗'}}</td>
            </tr>
        `;
    }});

    html += `</tbody></table>`;

    detailPanel.innerHTML = html;
    render();
}}

function render() {{
    const search = searchBox.value.toLowerCase();
    const symbol = symbolFilter.value;
    const regime = regimeFilter.value;
    const strategy = strategyFilter.value;

    const rows = DATA.filter(r => {{
        if (symbol && r.symbol !== symbol) return false;
        if (regime && r.regime !== regime) return false;
        if (strategy && r.strategy_family !== strategy) return false;
        return JSON.stringify(r).toLowerCase().includes(search);
    }});

    rowCount.innerText = rows.length;
    tbody.innerHTML = '';

    rows.forEach(r => {{
        const tr = document.createElement('tr');
        const key = `${{r.symbol}}|${{r.regime}}|${{r.strategy_family}}|${{r.strength_bucket}}`;

        tr.className = 'clickable' + (key === selectedKey ? ' selected' : '');

        tr.onclick = () => showDetails(r.symbol, r.regime, r.strategy_family, r.strength_bucket);

        tr.innerHTML = `
            <td>${{r.scope}}</td>
            <td>${{r.symbol}}</td>
            <td>${{r.regime}}</td>
            <td>${{r.strategy_family}}</td>
            <td>${{r.strength_bucket}}</td>
            <td>${{r.signals}}</td>
            <td class="${{heat(+r.win_pct_0)}}">${{fmt(r.win_pct_0)}}</td>
            <td class="${{heat(+r.win_pct_1)}}">${{fmt(r.win_pct_1)}}</td>
            <td class="${{heat(+r.win_pct_2)}}">${{fmt(r.win_pct_2)}}</td>
            <td class="${{heat(+r.win_pct_3)}}">${{fmt(r.win_pct_3)}}</td>
        `;

        tbody.appendChild(tr);
    }});
}}

searchBox.addEventListener('input', render);
symbolFilter.addEventListener('change', render);
regimeFilter.addEventListener('change', render);
strategyFilter.addEventListener('change', render);

render();
</script>

</body>
</html>
"""

    Path(OUTPUT_HTML).write_text(html, encoding="utf-8")
    print("Saved:", OUTPUT_HTML)


if __name__ == "__main__":
    main()
