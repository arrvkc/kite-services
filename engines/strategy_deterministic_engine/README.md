# Strategy Engine v2.0

This project implements the uploaded Strategy Engine v2.0 specification.

## Run

```bash
python run_strategy_engine_for_symbol.py examples/sample_input.json
python run_strategy_engine_batch.py examples/sample_batch_input.json
```

## Assumptions used only for four under-closed areas

1. Same family and different contract month:
   - If final strategy strength is at least 60, publish the new month immediately as stable.
   - Otherwise keep the previous committed month and emit `pending_switch`.
   - A second consecutive observation of the same month switch confirms it.

2. Pending confirmation mechanics:
   - The current evaluation counts as the first observation.
   - `pending_counter = 1` is stored on the first pending observation.
   - A matching second consecutive observation confirms the change.
   - During pending, the previously committed family and month remain publicly published.

3. Schema validation failure:
   - The payload is blocked from publication.
   - The engine returns a runtime-blocked result with the blocked payload and runtime error details.

4. CLI input shape:
   - Single-symbol runner accepts one JSON object.
   - Batch runner accepts an array of the same object shape.

## Input object shape

```json
{
  "instrument": "SBIN",
  "asof_time": "2026-04-19T15:30:00+05:30",
  "label": "UP",
  "confidence": 0.74,
  "aggregate_score": 48.0,
  "internal_state": "CLASSIFIABLE",
  "trend_history_w5": [
    {"label": "UP", "confidence": 0.71, "aggregate_score": 42.0},
    {"label": "UP", "confidence": 0.68, "aggregate_score": 39.0},
    {"label": "FLAT", "confidence": 0.52, "aggregate_score": 12.0},
    {"label": "UP", "confidence": 0.73, "aggregate_score": 44.0},
    {"label": "UP", "confidence": 0.74, "aggregate_score": 48.0}
  ],
  "dte_near_month": 8,
  "next_month_available": true,
  "dte_next_month": 29,
  "in_universe": true,
  "prior_committed_state": null
}
```
