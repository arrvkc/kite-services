#!/usr/bin/env bash
set -euo pipefail

docker exec -i postgres psql -U postgres -d atms -v ON_ERROR_STOP=1 <<'SQL'

INSERT INTO signal_trade_entries (
    entry_date,
    symbol,
    entry_price,
    regime_bucket,
    strategy_family,
    strength,
    previous_strength,
    transition,
    top_n,
    contract_month_selection,
    expiry_date,
    source_run_date
)
WITH base AS (
    SELECT
        s.run_date,
        s.symbol,
        (
            SELECT th.close
            FROM trend_history_fo_universe th
            WHERE th.symbol = s.symbol
              AND th.trade_date <= s.run_date
            ORDER BY th.trade_date DESC
            LIMIT 1
        ) AS entry_price,
        s.regime_bucket,
        s.strategy_family,
        s.final_strategy_strength AS strength,
        LAG(s.final_strategy_strength) OVER (
            PARTITION BY s.symbol
            ORDER BY s.run_date
        ) AS previous_strength,
        s.strategy_transition_state AS transition,
        s.include_in_top_n AS top_n,
        s.contract_month_selection,
        CASE
            WHEN s.contract_month_selection = 'NEAR_MONTH'
                THEN s.run_date + COALESCE(s.dte_near_month, 10)
            WHEN s.contract_month_selection = 'NEXT_MONTH'
                THEN s.run_date + COALESCE(s.dte_next_month, 10)
            ELSE s.run_date + 10
        END AS expiry_date
    FROM strategy_deterministic_engine_batch_results s
)
SELECT
    run_date,
    symbol,
    entry_price,
    regime_bucket,
    strategy_family,
    strength,
    previous_strength,
    transition,
    top_n,
    contract_month_selection,
    expiry_date,
    run_date
FROM base
WHERE strength >= 60
  AND (previous_strength < 60 OR previous_strength IS NULL)
  AND entry_price IS NOT NULL
ON CONFLICT DO NOTHING;


INSERT INTO signal_trade_observations (
    trade_entry_id,
    trade_date,
    close,
    return_pct,
    favorable_return_pct,
    adverse_return_pct,
    target_hit
)
WITH raw AS (
    SELECT
        e.id AS trade_entry_id,
        e.strategy_family,
        e.entry_price,
        th.trade_date,
        th.close,
        ROUND(((th.close - e.entry_price) / e.entry_price) * 100, 2) AS return_pct
    FROM signal_trade_entries e
    JOIN trend_history_fo_universe th
      ON th.symbol = e.symbol
     AND th.trade_date > e.entry_date
     AND th.trade_date <= e.expiry_date
),
scored AS (
    SELECT
        trade_entry_id,
        trade_date,
        close,
        return_pct,

        CASE
            WHEN strategy_family IN ('BULL_CALL_SPREAD', 'BULL_PUT_SPREAD')
                THEN MAX(return_pct) OVER (PARTITION BY trade_entry_id ORDER BY trade_date)
            WHEN strategy_family IN ('BEAR_CALL_SPREAD', 'BEAR_PUT_SPREAD')
                THEN MAX(-return_pct) OVER (PARTITION BY trade_entry_id ORDER BY trade_date)
            ELSE NULL
        END AS favorable_return_pct,

        CASE
            WHEN strategy_family IN ('BULL_CALL_SPREAD', 'BULL_PUT_SPREAD')
                THEN MIN(return_pct) OVER (PARTITION BY trade_entry_id ORDER BY trade_date)
            WHEN strategy_family IN ('BEAR_CALL_SPREAD', 'BEAR_PUT_SPREAD')
                THEN MIN(-return_pct) OVER (PARTITION BY trade_entry_id ORDER BY trade_date)
            ELSE NULL
        END AS adverse_return_pct,

        CASE
            WHEN strategy_family IN ('BULL_CALL_SPREAD', 'BULL_PUT_SPREAD')
                 AND return_pct >= 2 THEN true
            WHEN strategy_family IN ('BEAR_CALL_SPREAD', 'BEAR_PUT_SPREAD')
                 AND return_pct <= -2 THEN true
            ELSE false
        END AS target_hit
    FROM raw
)
SELECT
    trade_entry_id,
    trade_date,
    close,
    return_pct,
    favorable_return_pct,
    adverse_return_pct,
    target_hit
FROM scored
ON CONFLICT DO NOTHING;


INSERT INTO signal_trade_outcomes (
    trade_entry_id,
    expiry_date,
    best_favorable_return_pct,
    worst_adverse_return_pct,
    target_hit,
    target_hit_date,
    days_to_target,
    expiry_close,
    expiry_return_pct,
    final_result
)
WITH latest_market AS (
    SELECT MAX(trade_date) AS latest_trade_date
    FROM trend_history_fo_universe
),
first_hit AS (
    SELECT
        trade_entry_id,
        MIN(trade_date) AS target_hit_date
    FROM signal_trade_observations
    WHERE target_hit = true
    GROUP BY trade_entry_id
),
latest_obs AS (
    SELECT DISTINCT ON (trade_entry_id)
        trade_entry_id,
        close AS expiry_close,
        return_pct AS expiry_return_pct,
        trade_date AS latest_observation_date
    FROM signal_trade_observations
    ORDER BY trade_entry_id, trade_date DESC
),
summary AS (
    SELECT
        e.id AS trade_entry_id,
        e.expiry_date,
        MAX(o.favorable_return_pct) AS best_favorable_return_pct,
        MIN(o.adverse_return_pct) AS worst_adverse_return_pct,
        BOOL_OR(o.target_hit) AS target_hit,
        fh.target_hit_date,
        CASE
            WHEN fh.target_hit_date IS NULL THEN NULL
            ELSE (
                SELECT COUNT(*)
                FROM signal_trade_observations ox
                WHERE ox.trade_entry_id = e.id
                  AND ox.trade_date <= fh.target_hit_date
            )
        END AS days_to_target,
        lo.expiry_close,
        lo.expiry_return_pct,
        CASE
            WHEN BOOL_OR(o.target_hit) THEN 'WORKED'
            WHEN lm.latest_trade_date >= e.expiry_date THEN 'NOT_WORKED'
            ELSE 'OPEN'
        END AS final_result
    FROM signal_trade_entries e
    JOIN signal_trade_observations o
      ON o.trade_entry_id = e.id
    LEFT JOIN first_hit fh
      ON fh.trade_entry_id = e.id
    LEFT JOIN latest_obs lo
      ON lo.trade_entry_id = e.id
    CROSS JOIN latest_market lm
    GROUP BY
        e.id,
        e.expiry_date,
        fh.target_hit_date,
        lo.expiry_close,
        lo.expiry_return_pct,
        lm.latest_trade_date
)
SELECT *
FROM summary
ON CONFLICT (trade_entry_id) DO UPDATE SET
    expiry_date = EXCLUDED.expiry_date,
    best_favorable_return_pct = EXCLUDED.best_favorable_return_pct,
    worst_adverse_return_pct = EXCLUDED.worst_adverse_return_pct,
    target_hit = EXCLUDED.target_hit,
    target_hit_date = EXCLUDED.target_hit_date,
    days_to_target = EXCLUDED.days_to_target,
    expiry_close = EXCLUDED.expiry_close,
    expiry_return_pct = EXCLUDED.expiry_return_pct,
    final_result = EXCLUDED.final_result,
    updated_at = now();

SQL

echo "Signal research DB update completed."
