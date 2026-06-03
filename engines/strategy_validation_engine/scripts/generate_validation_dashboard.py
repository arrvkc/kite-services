from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


REPORT_CSV = (
    "data/strategy_validation/"
    "directional_accuracy_report.csv"
)

OUTPUT_HTML = (
    "data/strategy_validation/"
    "validation_dashboard.html"
)


def main():

    df = pd.read_csv(REPORT_CSV)

    data_json = json.dumps(
        df.to_dict(orient="records")
    )

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">

<title>
Deriviq Research Dashboard
</title>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 20px;
    background: #f8fafc;
}}

h1 {{
    margin-bottom: 20px;
}}

.toolbar {{
    display: flex;
    gap: 10px;
    margin-bottom: 20px;
}}

select,
input {{
    padding: 8px;
}}

.card-container {{
    display: flex;
    gap: 20px;
    margin-bottom: 20px;
}}

.card {{
    background: white;
    border-radius: 8px;
    padding: 16px;
    min-width: 220px;
    box-shadow:
        0 1px 4px
        rgba(0,0,0,.1);
}}

.card-title {{
    font-size: 12px;
    color: #666;
}}

.card-value {{
    font-size: 28px;
    font-weight: bold;
}}

table {{
    width: 100%;
    border-collapse: collapse;
    background: white;
}}

th,
td {{
    border: 1px solid #ddd;
    padding: 8px;
}}

th {{
    background: #f1f5f9;
}}

tr:hover {{
    background: #f8fafc;
}}

.green {{
    background:#16a34a;
    color:white;
}}

.lgreen {{
    background:#65a30d;
    color:white;
}}

.yellow {{
    background:#ca8a04;
    color:white;
}}

.red {{
    background:#dc2626;
    color:white;
}}

</style>
</head>

<body>

<h1>
Deriviq Research Dashboard
</h1>

<div class="toolbar">

<input
    id="searchBox"
    placeholder="Search..."
>

<select id="symbolFilter">
<option value="">
All Symbols
</option>
</select>

<select id="regimeFilter">
<option value="">
All Regimes
</option>
</select>

<select id="strategyFilter">
<option value="">
All Strategies
</option>
</select>

</div>

<div class="card-container">

<div class="card">
<div class="card-title">
Rows
</div>
<div
class="card-value"
id="rowCount"
>
0
</div>
</div>

<div class="card">
<div class="card-title">
Symbols
</div>
<div
class="card-value"
id="symbolCount"
>
0
</div>
</div>

</div>

<table>

<thead>

<tr>
<th>Scope</th>
<th>Symbol</th>
<th>Regime</th>
<th>Strategy</th>
<th>Bucket</th>
<th>Signals</th>
<th>0%</th>
<th>1%</th>
<th>2%</th>
<th>3%</th>
</tr>

</thead>

<tbody id="tbody">
</tbody>

</table>

<script>

const DATA = {data_json};

function heat(v) {{

    if(v >= 60)
        return 'green';

    if(v >= 50)
        return 'lgreen';

    if(v >= 40)
        return 'yellow';

    return 'red';
}}

const symbols =
[
    ...new Set(
        DATA.map(
            x => x.symbol
        )
    )
].sort();

const regimes =
[
    ...new Set(
        DATA.map(
            x => x.regime
        )
    )
].sort();

const strategies =
[
    ...new Set(
        DATA.map(
            x => x.strategy_family
        )
    )
].sort();

symbols.forEach(v => {{
    symbolFilter.innerHTML +=
        `<option>${{v}}</option>`;
}});

regimes.forEach(v => {{
    regimeFilter.innerHTML +=
        `<option>${{v}}</option>`;
}});

strategies.forEach(v => {{
    strategyFilter.innerHTML +=
        `<option>${{v}}</option>`;
}});

symbolCount.innerText =
    symbols.length;

function render() {{

    const search =
        searchBox.value
        .toLowerCase();

    const symbol =
        symbolFilter.value;

    const regime =
        regimeFilter.value;

    const strategy =
        strategyFilter.value;

    const rows =
        DATA.filter(r => {{

            if(
                symbol &&
                r.symbol !== symbol
            )
                return false;

            if(
                regime &&
                r.regime !== regime
            )
                return false;

            if(
                strategy &&
                r.strategy_family !== strategy
            )
                return false;

            return JSON.stringify(r)
                .toLowerCase()
                .includes(search);

        }});

    rowCount.innerText =
        rows.length;

    tbody.innerHTML = '';

    rows.forEach(r => {{

        const tr =
            document.createElement(
                'tr'
            );

        tr.innerHTML = `
        <td>${{r.scope}}</td>
        <td>${{r.symbol}}</td>
        <td>${{r.regime}}</td>
        <td>${{r.strategy_family}}</td>
        <td>${{r.strength_bucket}}</td>
        <td>${{r.signals}}</td>

        <td class="${{heat(+r.win_pct_0)}}">
            ${{
                (+r.win_pct_0)
                .toFixed(1)
            }}
        </td>

        <td class="${{heat(+r.win_pct_1)}}">
            ${{
                (+r.win_pct_1)
                .toFixed(1)
            }}
        </td>

        <td class="${{heat(+r.win_pct_2)}}">
            ${{
                (+r.win_pct_2)
                .toFixed(1)
            }}
        </td>

        <td class="${{heat(+r.win_pct_3)}}">
            ${{
                (+r.win_pct_3)
                .toFixed(1)
            }}
        </td>
        `;

        tbody.appendChild(tr);

    }});

}}

searchBox.addEventListener(
    'input',
    render
);

symbolFilter.addEventListener(
    'change',
    render
);

regimeFilter.addEventListener(
    'change',
    render
);

strategyFilter.addEventListener(
    'change',
    render
);

render();

</script>

</body>
</html>
"""

    output = Path(OUTPUT_HTML)

    output.write_text(
        html,
        encoding="utf-8"
    )

    print()
    print("=" * 80)
    print("Saved:", output)
    print("=" * 80)


if __name__ == "__main__":
    main()
