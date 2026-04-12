# Trend Identifier Kite Runner Usage

## File placement

Place the files at:

- `engines/trend_identifier/trend_identifier/runners/equity_trend_runner.py`
- `engines/trend_identifier/scripts/run_equity_trend.py`

## Dependency

Install Kite Connect if it is not already available:

```bash
pip install kiteconnect
```

## What this uses

The script uses your existing `services/kite_credentials_service.py` to fetch:

- `kite_api_key`
- `kite_access_token`

for a given Zerodha user id.

## Single stock

From repo root:

```bash
cd /Users/chakravarthi/kite_services
python engines/trend_identifier/scripts/run_equity_trend.py OMK569 POWERINDIA --pretty
```

## Multiple stocks

```bash
cd /Users/chakravarthi/kite_services
python engines/trend_identifier/scripts/run_equity_trend.py OMK569 POWERINDIA RELIANCE INFY TCS
```

## Summary only

```bash
python engines/trend_identifier/scripts/run_equity_trend.py OMK569 POWERINDIA RELIANCE --summary-only
```

## Custom lookback windows

```bash
python engines/trend_identifier/scripts/run_equity_trend.py OMK569 POWERINDIA --daily-lookback-days 1000 --hourly-lookback-days 150
```

## Output

The summary table includes:

- symbol
- exchange
- tradingsymbol
- instrument_token
- label
- confidence
- aggregate_score
- internal_state

## Notes

- This runner currently targets cash equities.
- Weekly bars are resampled from Kite day candles.
- Hourly bars are fetched using the `60minute` interval.
- The engine still enforces its own eligibility gates. If history or data quality is insufficient, it can return `FLAT` with `UNCLASSIFIABLE`.
