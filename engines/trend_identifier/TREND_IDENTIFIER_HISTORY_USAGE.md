# Trend Identifier History Runner Usage

## File placement

Place the files at:

- `engines/trend_identifier/trend_identifier/runners/equity_trend_history_runner.py`
- `engines/trend_identifier/trend_identifier/scripts/run_equity_trend_history.py`

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

## Example

From repo root:

```bash
cd /Users/chakravarthi/kite_services
python engines/trend_identifier/trend_identifier/scripts/run_equity_trend_history.py OMK569 CIPLA --history-days 60
```

## Save to CSV

```bash
python engines/trend_identifier/trend_identifier/scripts/run_equity_trend_history.py OMK569 CIPLA --history-days 60 --csv-out outputs/cipla_trend_history.csv
```

## Output columns

- `date`
- `asof_time`
- `symbol`
- `exchange`
- `tradingsymbol`
- `instrument_token`
- `label`
- `confidence`
- `aggregate_score`
- `internal_state`
- `transition_state`
- `weekly_label`
- `weekly_score`
- `daily_label`
- `daily_score`
- `hourly_label`
- `hourly_score`
- `reason_codes`

## Notes

- The history is built at the last hourly candle of each trading day.
- The runner takes the last `N` trading-day evaluation cut points from hourly data.
- Weekly and daily bars are truncated to each evaluation timestamp before calling the engine.
- This runner currently targets cash equities.
