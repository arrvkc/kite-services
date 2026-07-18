from __future__ import annotations

import csv
import html
import os
import smtplib
import subprocess
from email.message import EmailMessage
from io import StringIO


def query_csv(sql: str) -> list[dict]:
    cmd = [
        "docker", "exec", "-i", "postgres",
        "psql", "-U", "postgres", "-d", "atms",
        "-c", f"COPY ({sql}) TO STDOUT WITH CSV HEADER",
    ]
    out = subprocess.check_output(cmd, text=True)
    return list(csv.DictReader(StringIO(out)))


def esc(value) -> str:
    return html.escape("" if value is None else str(value))


def td_class(header: str, value) -> str:
    h = header.lower()
    v = "" if value is None else str(value)

    if h in {"worked", "not_worked", "open", "total", "trades", "days_to_target", "avg_days_to_target"}:
        return "center"

    if "pct" in h or "move" in h or "return" in h or "rate" in h:
        try:
            n = float(v)
            if n > 0:
                return "right positive"
            if n < 0:
                return "right negative"
        except Exception:
            pass
        return "right"

    return "left"


def render_badge(value) -> str:
    v = str(value)
    cls = {
        "WORKED": "badge badge-worked",
        "NOT_WORKED": "badge badge-failed",
        "OPEN": "badge badge-open",
    }.get(v, "badge badge-neutral")
    return f"<span class='{cls}'>{esc(v)}</span>"


def render_table(title: str, rows: list[dict]) -> str:
    if not rows:
        return f"""
        <tr>
          <td class="section">
            <div class="section-title">{esc(title)}</div>
            <div class="muted">No rows.</div>
          </td>
        </tr>
        """

    headers = list(rows[0].keys())

    thead = "".join(f"<th>{esc(h)}</th>" for h in headers)
    tbody = ""

    for row in rows:
        tbody += "<tr>"
        for h in headers:
            value = row.get(h, "")
            content = render_badge(value) if h.lower() in {"final_result", "result"} else esc(value)
            tbody += f"<td class='{td_class(h, value)}'>{content}</td>"
        tbody += "</tr>"

    return f"""
    <tr>
      <td class="section">
        <div class="section-title">{esc(title)}</div>
        <table class="report-table" role="presentation" cellpadding="0" cellspacing="0">
          <thead><tr>{thead}</tr></thead>
          <tbody>{tbody}</tbody>
        </table>
      </td>
    </tr>
    """


def render_kpi(label: str, value: str, tone: str = "") -> str:
    return f"""
    <td class="kpi-cell">
      <table class="kpi-card" role="presentation" cellpadding="0" cellspacing="0">
        <tr><td class="kpi-label">{esc(label)}</td></tr>
        <tr><td class="kpi-value {tone}">{esc(value)}</td></tr>
      </table>
    </td>
    """


def main() -> None:
    freshness = query_csv("""
        SELECT
            (SELECT MAX(run_date) FROM strategy_deterministic_engine_batch_results) AS latest_strategy_date,
            (SELECT MAX(trade_date) FROM trend_history_fo_universe) AS latest_trend_date
    """)[0]

    if freshness["latest_strategy_date"] != freshness["latest_trend_date"]:
        raise SystemExit(
            f"Strategy data not fresh. "
            f"strategy={freshness['latest_strategy_date']} "
            f"trend={freshness['latest_trend_date']}"
        )

    summary = query_csv("""
        SELECT final_result, COUNT(*) AS trades
        FROM signal_trade_outcomes
        GROUP BY final_result
        ORDER BY final_result
    """)

    strategy = query_csv("""
        SELECT
            e.strategy_family,
            o.final_result,
            COUNT(*) AS trades
        FROM signal_trade_entries e
        JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        GROUP BY e.strategy_family, o.final_result
        ORDER BY e.strategy_family, o.final_result
    """)

    strength = query_csv("""
        SELECT
            FLOOR(strength / 10) * 10 AS strength_bucket,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE final_result = 'WORKED') AS worked,
            COUNT(*) FILTER (WHERE final_result = 'NOT_WORKED') AS not_worked,
            COUNT(*) FILTER (WHERE final_result = 'OPEN') AS open,
            ROUND(
                COUNT(*) FILTER (WHERE final_result = 'WORKED')::numeric
                / NULLIF(COUNT(*) FILTER (WHERE final_result IN ('WORKED','NOT_WORKED')), 0)
                * 100,
                2
            ) AS closed_hit_rate_pct
        FROM signal_trade_entries e
        JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        GROUP BY FLOOR(strength / 10) * 10
        ORDER BY strength_bucket
    """)

    quality = query_csv("""
        SELECT
            e.strategy_family,
            ROUND(AVG(o.best_favorable_return_pct), 2) AS avg_best_move,
            ROUND(AVG(o.worst_adverse_return_pct), 2) AS avg_worst_move,
            ROUND(AVG(o.days_to_target), 2) AS avg_days_to_target,
            COUNT(*) FILTER (WHERE o.final_result = 'WORKED') AS worked,
            COUNT(*) FILTER (WHERE o.final_result = 'NOT_WORKED') AS not_worked
        FROM signal_trade_entries e
        JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        WHERE o.final_result IN ('WORKED', 'NOT_WORKED')
        GROUP BY e.strategy_family
        ORDER BY avg_best_move DESC
    """)

    fast = query_csv("""
        SELECT
            e.symbol,
            e.strategy_family,
            COUNT(*) AS total,
            ROUND(AVG(o.days_to_target), 2) AS avg_days_to_target,
            ROUND(AVG(o.best_favorable_return_pct), 2) AS avg_best_move,
            ROUND(AVG(o.worst_adverse_return_pct), 2) AS avg_worst_move
        FROM signal_trade_entries e
        JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        WHERE o.final_result = 'WORKED'
        GROUP BY e.symbol, e.strategy_family
        HAVING COUNT(*) >= 2
        ORDER BY avg_days_to_target ASC
        LIMIT 25
    """)

    symbol_performance = query_csv("""
        SELECT
            e.symbol,
            COUNT(*) AS total_trades,
            COUNT(*) FILTER (WHERE o.final_result = 'WORKED') AS worked,
            COUNT(*) FILTER (WHERE o.final_result = 'NOT_WORKED') AS not_worked,
            COUNT(*) FILTER (WHERE o.final_result = 'OPEN') AS open,
            ROUND(
                COUNT(*) FILTER (WHERE o.final_result = 'WORKED')::numeric
                / NULLIF(COUNT(*) FILTER (WHERE o.final_result IN ('WORKED','NOT_WORKED')), 0)
                * 100,
                2
            ) AS hit_rate_pct,
            ROUND(AVG(o.best_favorable_return_pct), 2) AS avg_best_move,
            ROUND(AVG(o.worst_adverse_return_pct), 2) AS avg_worst_move
        FROM signal_trade_entries e
        JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        GROUP BY e.symbol
        HAVING COUNT(*) >= 2
        ORDER BY hit_rate_pct DESC NULLS LAST, total_trades DESC
        LIMIT 50
    """)

    recent_entries = query_csv("""
        SELECT
            e.entry_date,
            e.symbol,
            e.entry_price,
            e.strategy_family,
            e.strength,
            e.previous_strength,
            e.transition,
            e.top_n,
            o.final_result,
            o.best_favorable_return_pct,
            o.worst_adverse_return_pct,
            o.days_to_target
        FROM signal_trade_entries e
        LEFT JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        ORDER BY e.entry_date DESC, e.strength DESC
        LIMIT 50
    """)

    open_trades = query_csv("""
        SELECT
            e.entry_date,
            e.symbol,
            e.entry_price,
            e.strategy_family,
            e.strength,
            o.best_favorable_return_pct,
            o.worst_adverse_return_pct,
            o.expiry_return_pct,
            o.final_result
        FROM signal_trade_entries e
        JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        WHERE o.final_result = 'OPEN'
        ORDER BY e.entry_date DESC, e.strength DESC
        LIMIT 50
    """)

    not_worked = query_csv("""
        SELECT
            e.entry_date,
            e.symbol,
            e.entry_price,
            e.strategy_family,
            e.strength,
            o.best_favorable_return_pct,
            o.worst_adverse_return_pct,
            o.expiry_return_pct,
            o.final_result
        FROM signal_trade_entries e
        JOIN signal_trade_outcomes o ON o.trade_entry_id = e.id
        WHERE o.final_result = 'NOT_WORKED'
        ORDER BY e.entry_date DESC, e.strength DESC
        LIMIT 50
    """)

    worked = int(next((r["trades"] for r in summary if r["final_result"] == "WORKED"), 0))
    failed = int(next((r["trades"] for r in summary if r["final_result"] == "NOT_WORKED"), 0))
    open_count = int(next((r["trades"] for r in summary if r["final_result"] == "OPEN"), 0))
    closed = worked + failed
    hit_rate = round((worked / closed) * 100, 2) if closed else 0

    html_body = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{
    margin: 0;
    padding: 0;
    background: #eef2f7;
    font-family: Arial, sans-serif;
    color: #172033;
}}
.wrapper {{
    width: 100%;
    background: #eef2f7;
    padding: 24px 0;
}}
.container {{
    width: 1080px;
    max-width: 1080px;
    margin: 0 auto;
    background: #ffffff;
    border: 1px solid #dbe3ef;
    border-radius: 14px;
}}
.hero {{
    background: #0f172a;
    color: #ffffff;
    padding: 28px 34px;
}}
.hero-title {{
    font-size: 28px;
    font-weight: bold;
    margin-bottom: 8px;
}}
.hero-subtitle {{
    color: #cbd5e1;
    font-size: 14px;
}}
.kpi-row {{
    padding: 18px 24px 8px 24px;
}}
.kpi-table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 10px;
}}
.kpi-cell {{
    width: 25%;
}}
.kpi-card {{
    width: 100%;
    background: #f8fafc;
    border: 1px solid #dbe3ef;
    border-radius: 12px;
}}
.kpi-label {{
    padding: 14px 14px 4px 14px;
    color: #64748b;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .06em;
    text-align: center;
}}
.kpi-value {{
    padding: 4px 14px 16px 14px;
    font-size: 30px;
    font-weight: bold;
    color: #0f172a;
    text-align: center;
}}
.section {{
    padding: 20px 34px;
}}
.section-title {{
    font-size: 18px;
    font-weight: bold;
    color: #111827;
    margin-bottom: 12px;
}}
.report-table {{
    width: 100%;
    border-collapse: collapse;
}}
.report-table th {{
    background: #f1f5f9;
    color: #334155;
    padding: 9px;
    border: 1px solid #dbe3ef;
    font-size: 12px;
    text-align: left;
}}
.report-table td {{
    padding: 8px;
    border: 1px solid #e2e8f0;
    font-size: 13px;
}}
.left {{ text-align: left; }}
.center {{ text-align: center; }}
.right {{ text-align: right; }}
.positive {{ color: #047857; font-weight: bold; }}
.negative {{ color: #b91c1c; font-weight: bold; }}
.badge {{
    display: inline-block;
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: bold;
}}
.badge-worked {{ background: #dcfce7; color: #166534; }}
.badge-failed {{ background: #fee2e2; color: #991b1b; }}
.badge-open {{ background: #dbeafe; color: #1d4ed8; }}
.badge-neutral {{ background: #e5e7eb; color: #374151; }}
.footer {{
    padding: 22px 34px 32px 34px;
    color: #64748b;
    font-size: 12px;
}}
.muted {{
    color: #64748b;
    font-size: 13px;
}}
</style>
</head>
<body>
<div class="wrapper">
<table class="container" role="presentation" cellpadding="0" cellspacing="0">
<tr>
<td class="hero">
    <div class="hero-title">Signal Research Daily Report</div>
    <div class="hero-subtitle">Latest strategy date: <b>{esc(freshness["latest_strategy_date"])}</b></div>
</td>
</tr>

<tr>
<td class="kpi-row">
<table class="kpi-table" role="presentation" cellpadding="0" cellspacing="0">
<tr>
{render_kpi("Worked", str(worked), "positive")}
{render_kpi("Not Worked", str(failed), "negative")}
{render_kpi("Open", str(open_count))}
{render_kpi("Closed Hit Rate", str(hit_rate) + "%", "positive")}
</tr>
</table>
</td>
</tr>

{render_table("1. Outcome Summary", summary)}
{render_table("2. Strategy Family Outcomes", strategy)}
{render_table("3. Strength Bucket Hit Rate", strength)}
{render_table("4. Strategy Quality Metrics", quality)}
{render_table("5. Fast Clean Responders", fast)}
{render_table("6. Symbol Performance", symbol_performance)}
{render_table("7. Recent Trade Entries", recent_entries)}
{render_table("8. Open Trades", open_trades)}
{render_table("9. Failed / Not Worked Trades", not_worked)}

<tr>
<td class="footer">
Decision-support report only. Not an automated trade instruction.
</td>
</tr>
</table>
</div>
</body>
</html>"""

    to_addr = os.getenv("REPORT_MAIL_TO", "").strip()
    from_addr = os.getenv("REPORT_MAIL_FROM", "").strip()
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()

    if not all([to_addr, from_addr, smtp_host, smtp_user, smtp_password]):
        raise SystemExit("Missing REPORT_MAIL_TO/REPORT_MAIL_FROM or SMTP_* env vars.")

    msg = EmailMessage()
    msg["Subject"] = f"Signal Research Daily Report - {freshness['latest_strategy_date']}"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content("Please view this email in HTML format.")
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    print(f"Email sent to {to_addr}")


if __name__ == "__main__":
    main()
