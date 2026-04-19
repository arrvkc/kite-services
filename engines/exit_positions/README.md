# BFPL-CLI integrated package

## Where these files go

Copy these files into your repo at:

```text
engines/exit_positions/__init__.py
engines/exit_positions/models.py
engines/exit_positions/planner.py
engines/exit_positions/executor.py
engines/exit_positions/bulk_exit.py
services/bulk_exit_kite_adapter.py
```

## Existing project files this package expects

These files already exist in your repo and are used directly:

```text
services/kite_credentials_service.py
services/kite_market_data_service.py
```

## Important current behavior

For now, **all target clients must be passed explicitly with `--clients`**.
That means:
- `--scope client-list` -> exits the explicit client IDs you pass
- `--scope all-clients` -> currently also requires `--clients` and treats them as the full target set for this run

This matches your clarification that "all clients" will be supplied as explicit client IDs for the time being.

## Canonical dry-run example

```bash
python -m engines.exit_positions.bulk_exit   --scope all-clients   --clients OMK569,ABC123   --segment futures   --side all   --dry-run
```

## Canonical live example

```bash
python -m engines.exit_positions.bulk_exit   --scope all-clients   --clients OMK569,ABC123   --segment futures   --side all   --cancel-gtt   --parallel-workers 8   --execute   --confirm EXIT_FUTURES_AND_GTT   --reason "Emergency flatten"
```

## GTT note

This implementation cancels **single-leg active stop GTTs** safely.
It does **not** auto-cancel broad multi-leg target/stop combinations because deleting a two-leg GTT would also delete the target leg.
That is a deliberate safety choice.
