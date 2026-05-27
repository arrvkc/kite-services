from __future__ import annotations

import html
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

import pandas as pd


BASE = Path("/opt/kite_services/data/strategy_transition_reports")
CSV_PATH = BASE / "strategy_transition_scanner_latest.csv"
PUBLIC_BASE_URL = "https://eajee.in/reports/strategy-transition"


def esc(v) -> str:
    return html.escape("" if pd.isna(v) else str(v))


def classify_transition(row):
    prev_s = str(row.get("previous_strategy_family", ""))
    curr_s = str(row.get("current_strategy_family", ""))
    prev_r = str(row.get("previous_regime_bucket", ""))
    curr_r = str(row.get("current_regime_bucket", ""))
    change_types = str(row.get("change_types", ""))

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


def signal_quality(row):
    ct = str(row.get("change_types", ""))
    vol = row.get("volume_ratio_20d")
    pchg = row.get("price_change_pct")

    structural = ("STRATEGY_FAMILY_CHANGE" in ct) or ("REGIME_BUCKET_CHANGE" in ct)
    topn = "TOPN_ENTERED" in ct
    high_vol = pd.notna(vol) and float(vol) >= 1.5
    strong_price = pd.notna(pchg) and abs(float(pchg)) >= 2

    if topn and high_vol:
        return "Very High"
    if structural and high_vol and strong_price:
        return "High"
    if structural and (high_vol or strong_price):
        return "Medium"
    return "Low"


def badge(value: str) -> str:
    colors = {
        "Very High": ("#fee2e2", "#991b1b"),
        "High": ("#ffedd5", "#9a3412"),
        "Medium": ("#fef9c3", "#854d0e"),
        "Low": ("#e0f2fe", "#075985"),
    }
    bg, fg = colors.get(value, ("#e5e7eb", "#374151"))
    return f"<span style='background:{bg};color:{fg};padding:4px 9px;border-radius:999px;font-size:12px;font-weight:700;'>{esc(value)}</span>"


def main() -> None:
    df = pd.read_csv(CSV_PATH)

    for col in ["price_change_pct", "volume_ratio_20d", "strength_delta", "confidence_delta"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["transition_direction"] = df.apply(classify_transition, axis=1)
    df["signal_quality"] = df.apply(signal_quality, axis=1)

    important = df[df["change_types"].fillna("").str.contains(
        "STRATEGY_FAMILY_CHANGE|REGIME_BUCKET_CHANGE|TOPN_ENTERED", na=False
    )].copy()

    high_priority = important[important["signal_quality"].isin(["Very High", "High"])].copy()

    run_date = str(df["run_date"].max())
    total = len(df)
    important_count = len(important)
    high_priority_count = len(high_priority)
    very_high = int((df["signal_quality"] == "Very High").sum())
    high = int((df["signal_quality"] == "High").sum())
    topn_entered = int(df["change_types"].fillna("").str.contains("TOPN_ENTERED").sum())
    high_volume = int((df["volume_ratio_20d"] >= 1.5).sum())

    quality_order = {"Very High": 0, "High": 1, "Medium": 2, "Low": 3}
    df["_q"] = df["signal_quality"].map(quality_order)
    df["_abs_price"] = df["price_change_pct"].abs()
    top_rows = df.sort_values(["_q", "volume_ratio_20d", "_abs_price"], ascending=[True, False, False]).head(12)

    row_html = ""
    for _, r in top_rows.iterrows():
        pchg = r.get("price_change_pct")
        pcls = "#047857" if pd.notna(pchg) and pchg > 0 else "#b91c1c" if pd.notna(pchg) and pchg < 0 else "#475569"

        row_html += f"""
        <tr>
          <td style="padding:9px;border-bottom:1px solid #e5e7eb;font-weight:700;">{esc(r.get("symbol"))}</td>
          <td style="padding:9px;border-bottom:1px solid #e5e7eb;">{badge(str(r.get("signal_quality")))}</td>
          <td style="padding:9px;border-bottom:1px solid #e5e7eb;">{esc(r.get("transition_direction"))}</td>
          <td style="padding:9px;border-bottom:1px solid #e5e7eb;">{esc(r.get("current_strategy_family"))}</td>
          <td style="padding:9px;border-bottom:1px solid #e5e7eb;text-align:center;">{esc(r.get("current_strength"))}</td>
          <td style="padding:9px;border-bottom:1px solid #e5e7eb;text-align:right;color:{pcls};font-weight:700;">{'' if pd.isna(pchg) else f'{pchg:.2f}%'}</td>
          <td style="padding:9px;border-bottom:1px solid #e5e7eb;text-align:right;">{'' if pd.isna(r.get("volume_ratio_20d")) else f'{r.get("volume_ratio_20d"):.2f}x'}</td>
        </tr>
        """

    latest_url = f"{PUBLIC_BASE_URL}/latest.html"
    dated_url = f"{PUBLIC_BASE_URL}/{run_date}.html"
    archive_url = f"{PUBLIC_BASE_URL}/"

    body = f"""<!doctype html>
<html>
<body style="margin:0;background:#f6f8fb;font-family:Arial,sans-serif;color:#172033;">
  <div style="max-width:980px;margin:0 auto;padding:24px;">
    <div style="background:#0f172a;color:white;padding:24px;border-radius:18px;">
      <h1 style="margin:0 0 8px 0;font-size:26px;">Strategy Transition Dashboard Ready</h1>
      <div style="color:#cbd5e1;font-size:14px;">Run date: <b>{esc(run_date)}</b></div>
    </div>

    <table width="100%" cellpadding="0" cellspacing="0" style="margin:18px 0;border-spacing:10px;border-collapse:separate;">
      <tr>
        <td style="background:white;border-radius:14px;padding:16px;border:1px solid #e5e7eb;"><div style="color:#64748b;font-size:12px;">TOTAL</div><div style="font-size:28px;font-weight:800;">{total}</div></td>
        <td style="background:white;border-radius:14px;padding:16px;border:1px solid #e5e7eb;"><div style="color:#64748b;font-size:12px;">IMPORTANT</div><div style="font-size:28px;font-weight:800;">{important_count}</div></td>
        <td style="background:white;border-radius:14px;padding:16px;border:1px solid #e5e7eb;"><div style="color:#64748b;font-size:12px;">HIGH PRIORITY</div><div style="font-size:28px;font-weight:800;">{high_priority_count}</div></td>
        <td style="background:white;border-radius:14px;padding:16px;border:1px solid #e5e7eb;"><div style="color:#64748b;font-size:12px;">TOP-N ENTERED</div><div style="font-size:28px;font-weight:800;">{topn_entered}</div></td>
        <td style="background:white;border-radius:14px;padding:16px;border:1px solid #e5e7eb;"><div style="color:#64748b;font-size:12px;">HIGH VOLUME</div><div style="font-size:28px;font-weight:800;">{high_volume}</div></td>
      </tr>
    </table>

    <div style="background:white;border-radius:16px;padding:18px;border:1px solid #e5e7eb;margin-bottom:18px;">
      <h2 style="margin-top:0;font-size:18px;">Quality Summary</h2>
      <p>{badge("Very High")} <b>{very_high}</b> &nbsp;&nbsp; {badge("High")} <b>{high}</b></p>
      <p>
        <a href="{latest_url}" style="display:inline-block;background:#1d4ed8;color:white;padding:11px 16px;border-radius:10px;text-decoration:none;font-weight:700;">Open Latest Dashboard</a>
        <a href="{dated_url}" style="display:inline-block;background:#0f172a;color:white;padding:11px 16px;border-radius:10px;text-decoration:none;font-weight:700;margin-left:8px;">Open Dated Report</a>
        <a href="{archive_url}" style="display:inline-block;background:#f1f5f9;color:#0f172a;padding:11px 16px;border-radius:10px;text-decoration:none;font-weight:700;margin-left:8px;">Archive</a>
      </p>
    </div>

    <div style="background:white;border-radius:16px;padding:18px;border:1px solid #e5e7eb;">
      <h2 style="margin-top:0;font-size:18px;">Top Transition Rows</h2>
      <div style="overflow-x:auto;">
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
        <thead>
          <tr style="background:#eef2f7;">
            <th style="padding:9px;text-align:left;">Symbol</th>
            <th style="padding:9px;text-align:left;">Quality</th>
            <th style="padding:9px;text-align:left;">Transition</th>
            <th style="padding:9px;text-align:left;">Strategy</th>
            <th style="padding:9px;text-align:center;">Strength</th>
            <th style="padding:9px;text-align:right;">Price</th>
            <th style="padding:9px;text-align:right;">Volume</th>
          </tr>
        </thead>
        <tbody>{row_html}</tbody>
      </table>
      </div>
    </div>

    <p style="color:#64748b;font-size:12px;">Decision-support report only. Not an automated trade instruction.</p>
  </div>
</body>
</html>"""

    to_addr = os.getenv("REPORT_MAIL_TO", "").strip()
    from_addr = os.getenv("REPORT_MAIL_FROM", "").strip()
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()

    msg = EmailMessage()
    msg["Subject"] = f"Strategy Transition Dashboard Ready - {run_date}"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(f"Strategy Transition Dashboard Ready: {latest_url}")
    msg.add_alternative(body, subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    print(f"Transition dashboard email sent to {to_addr}")


if __name__ == "__main__":
    main()
