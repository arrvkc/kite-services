#!/bin/bash
set -euo pipefail

USER_ID="${1:-OMK569}"

docker exec -i postgres psql -U postgres -d atms <<SQL
\pset pager off

SELECT 'LATEST STOP STATES' AS section;

SELECT
  l.account_id,
  l.tradingsymbol,
  l.side,
  s.current_stop,
  s.trigger_price,
  s.limit_price,
  s.quantity,
  s.broker_order_status,
  s.source,
  s.updated_at
FROM stop_trade_lifecycles l
JOIN stop_states s ON s.trade_lifecycle_id = l.id
WHERE l.account_id = '${USER_ID}'
  AND l.tradingsymbol ILIKE '%%'
ORDER BY s.updated_at DESC;

SELECT 'LATEST STOP EVENTS' AS section;

SELECT
  l.account_id,
  l.tradingsymbol,
  l.side,
  e.event_type,
  e.old_stop,
  e.raw_stop,
  e.final_stop,
  e.trigger_price,
  e.limit_price,
  e.quantity,
  e.action_taken,
  e.reason,
  e.created_at
FROM stop_events e
JOIN stop_trade_lifecycles l ON e.trade_lifecycle_id = l.id
  AND l.tradingsymbol ILIKE '%%'
WHERE l.account_id = '${USER_ID}'
ORDER BY e.created_at DESC
LIMIT 30;
SQL
