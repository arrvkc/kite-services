from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate Strategy Validation Dashboard"
    )

    parser.add_argument(
        "--summary",
        default="data/strategy_validation/directional_accuracy_summary.csv",
    )

    parser.add_argument(
        "--report",
        default="data/strategy_validation/directional_accuracy_report.csv",
    )

    parser.add_argument(
        "--strength-lift",
        default="data/strategy_validation/strength_lift_report.csv",
    )

    parser.add_argument(
        "--output",
        default="data/strategy_validation/validation_dashboard.html",
    )

    return parser.parse_args()


def df_to_html(df):
    return df.to_html(
        index=False,
        classes="table",
        border=0,
    )


def main():
    args = parse_args()

    summary = pd.read_csv(args.summary)
    report = pd.read_csv(args.report)
    strength = pd.read_csv(args.strength_lift)

    all_report = report[
        report["scope"] == "ALL"
    ].copy()

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Strategy Validation Dashboard</title>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 20px;
}}

h1 {{
    margin-bottom: 10px;
}}

h2 {{
    margin-top: 40px;
}}

.table {{
    border-collapse: collapse;
    width: 100%;
}}

.table th,
.table td {{
    border: 1px solid #ddd;
    padding: 8px;
    text-align: right;
}}

.table th {{
    background: #f4f4f4;
}}

.table td:first-child,
.table th:first-child {{
    text-align: left;
}}

.table td:nth-child(2),
.table th:nth-child(2) {{
    text-align: left;
}}

#symbolSelect {{
    padding: 8px;
    font-size: 14px;
}}

</style>

<script>

function filterSymbol() {{

    const symbol =
        document.getElementById(
            "symbolSelect"
        ).value;

    const rows =
        document.querySelectorAll(
            "#symbolTable tbody tr"
        );

    rows.forEach(row => {{

        if (
            symbol === "ALL"
            ||
            row.dataset.symbol === symbol
        ) {{
            row.style.display = "";
        }}
        else {{
            row.style.display = "none";
        }}

    }});

}}

</script>

</head>

<body>

<h1>Strategy Validation Dashboard</h1>

<h2>Directional Accuracy Summary</h2>

{df_to_html(summary.round(2))}

<h2>Strength Lift Report</h2>

{df_to_html(strength.round(2))}

<h2>Universe Strength Analysis</h2>

{df_to_html(all_report.round(2))}

<h2>Symbol Drilldown</h2>

<select
    id="symbolSelect"
    onchange="filterSymbol()"
>
<option value="ALL">ALL</option>
"""

    symbols = sorted(
        report[
            report["scope"] == "SYMBOL"
        ]["symbol"].unique()
    )

    for symbol in symbols:
        html += (
            f'<option value="{symbol}">'
            f'{symbol}'
            '</option>'
        )

    html += """
</select>

<table
    id="symbolTable"
    class="table"
>
<thead>
<tr>
"""

    for col in report.columns:
        html += f"<th>{col}</th>"

    html += """
</tr>
</thead>
<tbody>
"""

    for _, row in report.iterrows():

        symbol = row["symbol"]

        html += (
            f'<tr data-symbol="{symbol}">'
        )

        for value in row:
            html += f"<td>{value}</td>"

        html += "</tr>"

    html += """
</tbody>
</table>

</body>
</html>
"""

    output_file = Path(args.output)

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file.write_text(
        html,
        encoding="utf-8",
    )

    print()
    print("=" * 80)
    print("Saved:", output_file)
    print("=" * 80)


if __name__ == "__main__":
    main()
