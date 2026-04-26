# Options Construction Engine — Locked Spec Delivery

This package adds:

```text
engines/options_construction_engine/
  constants.py
  models.py
  schema.py
  engine.py
  kite_adapter.py
  scripts/run_options_construction_engine_for_symbol.py
  examples/reference_input.json
  examples/reference_expected_output.json
  tests/test_engine.py
```

## Install / copy

Copy the `engines/options_construction_engine` folder into the root of your existing `/Users/chakravarthi/kite_services` repo.

## Run tests

```bash
cd /Users/chakravarthi/kite_services
PYTHONPATH=. pytest engines/options_construction_engine/tests -q
```

Expected result in this package:

```text
11 passed
```

## Important honesty note

The PDF specification has internal conflicts:

1. Section 10 says bid-ask spread percentage must be `<= 15%`.
   The PDF reference fixture uses:
   - 155 CE: `(3.5 - 3.0) / 3.25 = 15.38%`
   - 160 CE: `(2.0 - 1.5) / 1.75 = 28.57%`

   Strict production mode therefore rejects the PDF reference fixture for liquidity.

2. Section 6 says `NEXT_MONTH` must choose the expiry strictly after the near-month expiry.
   The PDF reference fixture provides only one expiry (`2026-05-28`) but expects it to be selected as `NEXT_MONTH`.

3. Section 8 says credit-spread width target is `max(2 strike steps, nearest strike distance to 2% of spot)`.
   With spot `150` and strike step `5`, that implies a width target of `10`, but the PDF expected output uses width `5`.

Because you asked for no `xfail`, the package contains:
- strict production default mode,
- explicit `reference_fixture_compatibility=True` mode used only to prove the PDF Section 22 expected JSON can be reproduced exactly.

Do not enable `reference_fixture_compatibility=True` for live production runs.

## Core usage

```python
from engines.options_construction_engine.engine import OptionsConstructionEngine

engine = OptionsConstructionEngine()
output = engine.construct(strategy_payload, option_chain)
```

The core engine does not fetch data and does not place orders.

## Kite live adapter

```python
from engines.options_construction_engine.kite_adapter import get_kite_client, KiteOptionChainAdapter

kite = get_kite_client("OMK569")
chain = KiteOptionChainAdapter(kite).build_option_chain("SBIN", asof_time=strategy_payload["asof_time"])
```

The adapter reads instruments and quotes only. It does not place orders.

## CLI

```bash
PYTHONPATH=.:services python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py OMK569 strategy_payload.json
```

With audit:

```bash
PYTHONPATH=.:services python engines/options_construction_engine/scripts/run_options_construction_engine_for_symbol.py OMK569 strategy_payload.json \
  --audit-log-path services/audit_logs/options_construction/sbin.json
```

## Determinism verification

Run the same input twice and compare JSON outputs:

```bash
PYTHONPATH=. pytest engines/options_construction_engine/tests/test_engine.py::test_deterministic_output_identical_for_identical_inputs -q
```

## Boundary

This engine:
- does not place orders,
- does not decide final quantity sizing,
- does not compute stop-loss,
- does not perform rollover,
- does not do margin optimization,
- does not use last price for construction economics.
