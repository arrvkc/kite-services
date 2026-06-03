from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

REPORT_CSV = "data/strategy_validation/directional_accuracy_report.csv"
VALIDATION_CSV = "data/strategy_validation/fo_universe_validation.csv"
OUTPUT_HTML = "data/strategy_validation/validation_dashboard.html"


def main():

    report_df = pd.read_csv(REPORT_CSV)

    signal_df = pd.read_csv(VALIDATION_CSV)

    signal_df["strength_bucket"] = pd.cut(
        signal_df["strength"],
        bins=[-1, 19, 39, 59, 79, 100],
        labels=[
            "00-19",
            "20-39",
            "40-59",
            "60-79",
            "80-100",
        ],
    )

    report_json = json.dumps(
        report_df.fillna("").to_dict(orient="records")
    )

    signal_json = json.dumps(
        signal_df.fillna("").to_dict(orient="records")
    )

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Deriviq Research Dashboard</title>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 20px;
}}

.toolbar {{
    display:flex;
    gap:10px;
    margin-bottom:20px;
}}

select,input {{
    padding:8px;
}}

table {{
    width:100%;
    border-collapse:collapse;
}}

th,td {{
    border:1px solid #ddd;
    padding:8px;
}}

th {{
    background:#f5f5f5;
}}

tr:hover {{
    background:#f8f8f8;
}}

.clickable {{
    cursor:pointer;
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

#modal {{
    display:none;
    position:fixed;
    top:0;
    left:0;
    width:100%;
    height:100%;
    background:rgba(0,0,0,.5);
    overflow:auto;
}}

#modal-content {{
    background:white;
    width:95%;
    margin:20px auto;
    padding:20px;
}}

.close-btn {{
    padding:8px 16px;
}}

</style>

</head>

<body>

<h1>Deriviq Research Dashboard</h1>

<div class="toolbar">

<input
id="searchBox"
placeholder="Search..."
>

<select id="symbolFilter">
<option value="">All Symbols</option>
</select>

<select id="regimeFilter">
<option value="">All Regimes</option>
</select>

<select id="strategyFilter">
<option value="">All Strategies</option>
</select>

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

<div id="modal">

<div id="modal-content">

<h2 id="modal-title">
Signal Details
</h2>

<button
class="close-btn"
onclick="closeModal()"
>
Close
</button>

<br><br>

<table>

<thead>
<tr>
<th>Date</th>
<th>Symbol</th>
<th>Strength</th>
<th>Confidence</th>
<th>Score</th>
<th>Return 5D</th>
</tr>
</thead>

<tbody id="detail-body">
</tbody>

</table>

</div>

</div>

<script>

const DATA = {report_json};

const SIGNALS = {signal_json};

function heat(v){{
    if(v >= 60) return 'green';
    if(v >= 50) return 'lgreen';
    if(v >= 40) return 'yellow';
    return 'red';
}}

const symbols =
[
    ...new Set(
        DATA.map(x=>x.symbol)
    )
].sort();

const regimes =
[
    ...new Set(
        DATA.map(x=>x.regime)
    )
].sort();

const strategies =
[
    ...new Set(
        DATA.map(x=>x.strategy_family)
    )
].sort();

symbols.forEach(v=>{{
    symbolFilter.innerHTML +=
    `<option>${{v}}</option>`;
}});

regimes.forEach(v=>{{
    regimeFilter.innerHTML +=
    `<option>${{v}}</option>`;
}});

strategies.forEach(v=>{{
    strategyFilter.innerHTML +=
    `<option>${{v}}</option>`;
}});

function closeModal(){{
    modal.style.display='none';
}}

function showDetails(
    symbol,
    regime,
    strategy,
    bucket
){{

    const rows =
        SIGNALS.filter(r =>

            r.symbol === symbol &&
            r.regime === regime &&
            r.strategy_family === strategy &&
            r.strength_bucket === bucket

        );

    modal.style.display='block';

    modalTitle =
        document.getElementById(
            'modal-title'
        );

    modalTitle.innerText =
        symbol +
        ' | ' +
        strategy +
        ' | ' +
        bucket;

    detailBody =
        document.getElementById(
            'detail-body'
        );

    detailBody.innerHTML='';

    rows
    .sort(
        (a,b)=>
        b.strength-a.strength
    )
    .forEach(r=>{{

        const tr =
            document.createElement(
                'tr'
            );

        tr.innerHTML = `
        <td>${{r.run_date}}</td>
        <td>${{r.symbol}}</td>
        <td>${{r.strength}}</td>
        <td>${{Number(r.confidence).toFixed(2)}}</td>
        <td>${{Number(r.score).toFixed(2)}}</td>
        <td>${{Number(r.return_5d_pct).toFixed(2)}}</td>
        `;

        detailBody.appendChild(
            tr
        );

    }});

}}

function render(){{

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
        DATA.filter(r=>{{

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

    tbody.innerHTML='';

    rows.forEach(r=>{{

        const tr =
            document.createElement(
                'tr'
            );

        tr.className =
            'clickable';

        tr.onclick =
            () => showDetails(
                r.symbol,
                r.regime,
                r.strategy_family,
                r.strength_bucket
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

    Path(OUTPUT_HTML).write_text(
        html,
        encoding="utf-8",
    )

    print("Saved:", OUTPUT_HTML)


if __name__ == "__main__":
    main()
