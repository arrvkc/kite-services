# Strategy Engine v1.2 package

This package implements a deterministic Python Strategy Engine layer derived from the uploaded Strategy Engine v1.2 specification.

## Files

- `strategy_engine/`: core package
- `tests/`: pytest unit tests

## CLI usage

Single instrument:

```bash
python -m strategy_engine.cli --input-file sample_single.json
```

Batch:

```bash
python -m strategy_engine.cli --batch --input-file sample_batch.json --output-file out.json
```

## Narrow integration assumption

The existing Trend Identifier implementation is **not** reimplemented here. The adapter in `strategy_engine/adapters/trend_identifier.py` accepts already-produced Trend Identifier payload dictionaries and maps them into Strategy Engine models.

## Conservative choices

- Duplicate payloads are treated as hard non-publishable evaluation inputs.
- Pending-state confirmation is implemented using the immediately prior saved pending candidate in `PreviousStrategyState`.
- Runtime persistence hooks are not implemented because repository interfaces were not provided; this package focuses on deterministic engine logic and payload generation.
