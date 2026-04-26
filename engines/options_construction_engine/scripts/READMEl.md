# Options Construction Runner Script README

## File

```text
engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py
```

## Purpose

This script is the command-line runner for the Options Construction Engine.

It can construct option strategies in four practical ways:

1. From a saved Strategy Engine JSON payload.
2. Directly from a single symbol.
3. From a comma-separated basket of symbols.
4. From the full F&O (Futures and Options) universe.

It is designed for analysis, testing, comparison, auditability, and controlled validation of strategy construction outputs.

The script does **not** place orders.

The current script supports direct symbols, basket runs, symbols-file runs, F&O universe discovery, table output, JSON output, automatic audit files, daily summary CSVs, bulk CSVs, and optional candidate comparison output through `--show-candidates`. These capabilities are visible in the uploaded runner script’s parser and helper functions. 

---

## What the script does

At a high level, the script performs this pipeline:

```text
Input
  ↓
Strategy Engine payload
  ↓
Kite option-chain adapter
  ↓
Options Construction Engine
  ↓
Selected strategy output
  ↓
Audit JSON + summary CSV + optional candidate comparison table
```

When run from a symbol, it internally performs:

```text
Symbol
  ↓
Trend / Strategy Engine adapter
  ↓
Strategy Engine evaluation
  ↓
Spot price, lot size, strike step enrichment
  ↓
Options Construction Engine
```

This removes the need to manually generate an intermediate strategy JSON file.

---

## Important Safety Principle

This script is an analysis and construction tool.

It does not place orders.

Even if the result is `CONSTRUCTED`, this should be treated as a candidate structure, not as an automatic trade instruction.

In `AFTER_HOURS_HISTORICAL` mode, `execution_ready` should always be false because the prices are historical proxies, not executable market quotes.

---

## Required working directory

Run all commands from the project root:

```bash
cd /Users/chakravarthi/kite_services
```

Use:

```bash
PYTHONPATH=.
```

Example:

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py ...
```

---

## Main Usage Modes

## 1. Single symbol mode

Use this for one stock or index.

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 IRFC \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

This internally builds the Strategy Engine payload and then runs the Options Construction Engine.

---

## 2. Single symbol with scored candidates

Use this when you want to compare the selected structure against other scored candidates.

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 POWERINDIA \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --show-candidates
```

Expected style:

```text
SYMBOL       STRATEGY             STATUS       EXPIRY       SPREAD                        WIDTH      NET     CR/W      PROFIT        LOSS       ROI      RR  SCORE   EXEC MODE                     ERROR
POWERINDIA   BULL_CALL_SPREAD     SELECTED     2026-05-26   S 35000CE / B 34000CE       1000.00   294.90   29.49%    35255.00    14745.00   239.10%    2.39     93  False AFTER_HOURS_HISTORICAL
POWERINDIA   BULL_CALL_SPREAD     SCORED       2026-05-26   S 34000CE / B 33000CE       1000.00   316.25   31.63%    34187.50    15812.50   216.21%    2.16     91  False AFTER_HOURS_HISTORICAL
POWERINDIA   BULL_CALL_SPREAD     SCORED       2026-05-26   S 33000CE / B 32000CE       1000.00   449.50   44.95%    27525.00    22475.00   122.47%    1.22     87  False AFTER_HOURS_HISTORICAL
```

`--show-candidates` shows scored candidates only. It does not show every rejected candidate in the main table.

Rejected candidate details are available in the audit JSON.

---

## 3. Basket mode using comma-separated symbols

Use `--symbols` for multiple symbols.

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --symbols NIFTY,RELIANCE,INFY,SBIN,IRFC,POWERINDIA \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

This prints one table header and one output row per symbol.

---

## 4. Basket mode with candidate comparison

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --symbols POWERINDIA,IRFC \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --show-candidates
```

Use this carefully. The output can become long because each symbol may print multiple candidate rows.

---

## 5. Symbols file mode

Create a file:

```bash
cat > symbols.txt <<'EOF'
NIFTY
RELIANCE
IRFC
POWERINDIA
SBIN
INFY
EOF
```

Run:

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --symbols-file symbols.txt \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

Blank lines and lines starting with `#` are ignored.

---

## 6. F&O universe mode

Run across all symbols with listed NFO (National Futures and Options) option contracts:

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --fo-universe \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

Strong recommendation: start with a limit first.

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --fo-universe \
  --limit 20 \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

Then increase gradually:

```bash
--limit 50
--limit 100
```

Full F&O universe runs can take significant time.

---

## 7. JSON payload mode

Use this if you already have a Strategy Engine JSON file.

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 irfc_strategy_output.json \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

This preserves the older two-step workflow for debugging.

---

## 8. JSON output mode

By default, output is table format.

To print the full wrapper JSON:

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 IRFC \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --output json
```

JSON output contains:

```text
run_context
strategy_result
construction_result
```

---

## 9. Save intermediate Strategy Engine JSON

Use this to inspect the internally generated Strategy Engine payload:

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 IRFC \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --save-strategy-json data/options_pipeline/IRFC_strategy_output.json
```

This is useful when validating:

```text
strategy_family
contract_month_selection
underlying_spot_price
lot_size
strike_step
reason_codes
final_strategy_strength
```

---

# Liquidity Modes

## LIVE_STRICT

```bash
--liquidity-mode LIVE_STRICT
```

Uses live Kite quote bid/ask.

Characteristics:

```text
intended for market hours
uses live bid/ask
strict freshness policy
can be execution_ready=true
```

Use this only when market data is live and reliable.

---

## AFTER_HOURS_HISTORICAL

```bash
--liquidity-mode AFTER_HOURS_HISTORICAL
```

Uses Kite historical last volume-positive candle close as a bid/ask proxy.

Characteristics:

```text
non-production testing mode
uses historical candle close
avoids synthetic pricing
avoids raw LTP-only pricing
execution_ready is always false
```

This is useful after market hours when live bid/ask is zero.

It should not be treated as executable pricing.

---

# Output Columns

The table output has these columns:

```text
SYMBOL
STRATEGY
STATUS
EXPIRY
SPREAD
WIDTH
NET
CR/W
PROFIT
LOSS
ROI
RR
SCORE
EXEC
MODE
ERROR
```

## SYMBOL

Instrument symbol.

Examples:

```text
IRFC
POWERINDIA
NIFTY
```

## STRATEGY

Strategy family selected by the Strategy Engine.

Examples:

```text
BULL_CALL_SPREAD
BEAR_CALL_SPREAD
BULL_PUT_SPREAD
BEAR_PUT_SPREAD
IRON_CONDOR
```

## STATUS

Normal mode:

```text
CONSTRUCTED
REJECTED
ERROR
```

Candidate mode:

```text
SELECTED
SCORED
```

`SELECTED` is the final chosen candidate.

`SCORED` is a valid candidate that reached scoring but lost ranking.

## EXPIRY

Selected option expiry.

Example:

```text
2026-05-26
```

## SPREAD

Readable option-leg display.

Examples:

```text
S 35000CE / B 34000CE
S 109CE / B 110CE
```

`S` means Sell.

`B` means Buy.

## WIDTH

Strike distance between legs.

Example:

```text
1000.00
1.00
500.00
```

## NET

Net premium.

For credit spreads:

```text
sell premium - buy premium
```

For debit spreads, this represents net debit according to the engine’s strategy logic.

## CR/W

Credit as percentage of width.

Formula:

```text
net_premium / width
```

Example:

```text
0.69 / 1 = 69%
```

This is one of the most important comparison columns.

## PROFIT

Maximum profit per lot.

## LOSS

Maximum loss per lot.

## ROI

Return on max risk.

Formula:

```text
max_profit_per_lot / max_loss_per_lot
```

Displayed as a percentage.

## RR

Reward / Risk ratio.

Formula:

```text
max_profit_per_lot / max_loss_per_lot
```

## SCORE

Construction score from the Options Construction Engine.

The score is based on components such as:

```text
liquidity
strike fit
reward/risk
bid/ask quality
expiry fit
```

## EXEC

Execution readiness.

In `AFTER_HOURS_HISTORICAL`, this should always be:

```text
False
```

## MODE

The liquidity/pricing mode used.

Examples:

```text
AFTER_HOURS_HISTORICAL
LIVE_STRICT
```

## ERROR

Shows script-level or construction-level error codes when applicable.

---

# CSV Outputs

## Daily Summary CSV

Every run appends to:

```text
services/audit_logs/options_construction/summary_YYYY-MM-DD.csv
```

Example:

```text
services/audit_logs/options_construction/summary_2026-04-26.csv
```

This stores one row per selected construction result.

It does not store every candidate printed by `--show-candidates`.

## Bulk CSV

Bulk runs also create:

```text
services/audit_logs/options_construction/bulk_YYYY-MM-DD_YYYYMMDD_HHMMSS.csv
```

Example:

```text
services/audit_logs/options_construction/bulk_2026-04-26_20260426_121821.csv
```

Use bulk CSVs to compare baskets and universe runs.

## Disable summary CSV

```bash
--no-summary-csv
```

Example:

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 IRFC \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --no-summary-csv
```

---

# Audit JSON

Each run creates an audit JSON file unless audit path is disabled in code.

Default path:

```text
services/audit_logs/options_construction/YYYY-MM-DD/SYMBOL_MODE_YYYYMMDD_HHMMSS.json
```

Audit JSON contains:

```text
input snapshot
option-chain snapshot hash
candidate generation list
liquidity-filtered candidate list
pricing/risk-filtered candidate list
scored candidates
rejected candidates
final selection
idempotency hash
runtime stage
```

Use audit JSON to answer:

```text
Why was this candidate selected?
Why were other candidates rejected?
Was the issue liquidity, OI, volume, credit, or reward/risk?
What was the score?
What was the selected candidate ID?
```

---

# Candidate Comparison

Use:

```bash
--show-candidates
```

This reads the audit file after construction and prints scored candidates in the same full table format.

Important:

```text
Candidate comparison depends on audit enrichment.
```

The audit must store, for each scored candidate:

```text
legs
economics
construction_score
```

If these fields are missing, candidate rows will not be complete.

---

# Rejected Candidates

Rejected candidates are not printed in the main full table.

To inspect rejections, open the audit JSON:

```bash
python -m json.tool services/audit_logs/options_construction/YYYY-MM-DD/SYMBOL_AFTER_HOURS_HISTORICAL_*.json
```

Look at:

```text
rejected_candidates
```

Common rejection reasons:

```text
LIQUIDITY_CHECK_FAILED
OPEN_INTEREST_TOO_LOW
VOLUME_TOO_LOW
CREDIT_TOO_LOW
INVALID_NET_PREMIUM
POOR_REWARD_RISK
NO_VALID_STRIKE_PAIR
```

---

# Important Model Assumption: Credit Rule Is Not Universal

The current credit-spread model effectively requires:

```text
net_credit >= 20% of spread width
```

This works well for some stock options but can reject index options such as NIFTY.

Important:

```text
High liquidity does not mean high credit.
```

NIFTY can be liquid but still fail a high credit-per-width rule.

This means the engine currently behaves like a:

```text
high credit-efficiency filter
```

not a:

```text
general options trade finder
```

Future improvements may include:

```text
adaptive credit rule
index-specific thresholds
volatility-aware thresholds
probability of profit
delta / IV enrichment
```

---

# Recommended Validation Workflow

## Step 1: One-symbol deep inspection

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 POWERINDIA \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --show-candidates
```

Check:

```text
selected row
candidate rows
score ordering
credit percentage
reward/risk
```

## Step 2: Small basket

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --symbols NIFTY,RELIANCE,INFY,SBIN,TATASTEEL,IRFC,POWERINDIA \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

Check:

```text
constructed vs rejected
credit_pct_width
score distribution
width behavior
index vs stock behavior
```

## Step 3: Limited F&O universe

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --fo-universe \
  --limit 20 \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

## Step 4: Review CSV

```bash
open services/audit_logs/options_construction/summary_$(date +%F).csv
```

or list bulk CSVs:

```bash
ls -lh services/audit_logs/options_construction/bulk_*.csv
```

## Step 5: Decide model changes only after data

Do not change scoring based on one symbol.

Review multiple symbols before deciding on:

```text
credit threshold
width target
score weights
index-vs-stock rule
probability-of-profit layer
```

---

# Troubleshooting

## `unrecognized arguments: --liquidity-mode`

The script is old.

Check:

```bash
grep -n "liquidity-mode" engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py
```

## `OptionsConstructionConfig has no attribute liquidity_mode`

`models.py` is old.

Check:

```bash
grep -n "liquidity_mode" engines/options_construction_engine/models.py
```

Expected:

```text
liquidity_mode: str = "LIVE_STRICT"
```

## Duplicate selected row in `--show-candidates`

Expected correct output should be:

```text
SELECTED
SCORED
SCORED
```

not:

```text
CONSTRUCTED
SELECTED
SCORED
SCORED
```

If duplicate appears, bulk-mode print logic still prints the normal row before candidate rows.

## No candidate rows shown

Possible causes:

```text
audit file missing
audit file not enriched
all candidates rejected before scoring
engine.py not updated to store scored candidate economics
```

Check the audit:

```bash
python -m json.tool services/audit_logs/options_construction/YYYY-MM-DD/SYMBOL_AFTER_HOURS_HISTORICAL_*.json
```

Look for:

```text
candidate_lists.scored
```

## F&O universe run is slow

Expected.

Use:

```bash
--limit 20
```

before a full run.

## Kite token error

Test credentials:

```bash
PYTHONPATH=.:services python services/kite_credentials_service.py XJ1877
```

Test profile:

```bash
PYTHONPATH=.:services python - <<'PY'
from kiteconnect import KiteConnect
from services.kite_credentials_service import get_kite_credentials

user_id = "XJ1877"
api_key, access_token = get_kite_credentials(user_id)
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

profile = kite.profile()
print("Kite token OK")
print(profile.get("user_id"), profile.get("user_name"))
PY
```

---

# Quick Reference

## One symbol

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 IRFC \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

## One symbol with candidates

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 IRFC \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --show-candidates
```

## Basket

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --symbols NIFTY,RELIANCE,IRFC \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

## F&O universe, limited

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 \
  --fo-universe \
  --limit 20 \
  --liquidity-mode AFTER_HOURS_HISTORICAL
```

## JSON output

```bash
PYTHONPATH=. python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py \
  XJ1877 IRFC \
  --from-symbol \
  --liquidity-mode AFTER_HOURS_HISTORICAL \
  --output json
```

---

# Suggested Next Enhancements

Recommended future enhancements:

```text
candidate CSV export
rejected candidate summary table
adaptive credit model
index-vs-stock classification
probability of profit
delta / IV enrichment
Google Sheet export
bulk run rate limiting
parallel execution with Kite API safety controls
```

The best next enhancement is probably:

```text
candidate CSV export
```

That would allow every scored candidate, not just the selected result, to be analyzed in Excel or Google Sheets.
