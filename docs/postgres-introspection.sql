-- Docket v0.1.0 revision-8 operational introspection.
-- Replace interval literals to match the deployed dispatcher and retention policy.

-- name: ready_backlog
SELECT run_id, tenant_id, graph_id, wake_at, claim_attempts
FROM docket_runs
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NULL
  AND wake_at <= CURRENT_TIMESTAMP
ORDER BY wake_at, id;

-- name: expired_claims
SELECT run_id, tenant_id, graph_id, claimed_at, claim_attempts
FROM docket_runs
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NOT NULL
  AND claimed_at < CURRENT_TIMESTAMP - INTERVAL '60 seconds'
ORDER BY claimed_at, id;

-- name: fresh_in_flight_claims
SELECT run_id, tenant_id, graph_id, claimed_at, claim_attempts
FROM docket_runs
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NOT NULL
  AND claimed_at >= CURRENT_TIMESTAMP - INTERVAL '60 seconds'
ORDER BY claimed_at, id;

-- name: poisoned_runs
SELECT run_id, tenant_id, graph_id, poisoned_at, poison_reason,
       claim_attempts, claim_abandons
FROM docket_runs
WHERE poisoned_at IS NOT NULL
ORDER BY poisoned_at, id;

-- name: oldest_wake
SELECT MIN(wake_at) AS oldest_wake
FROM docket_runs
WHERE status = 'running'
  AND poisoned_at IS NULL
  AND claim_token IS NULL;

-- name: invalid_unscheduled_rows
SELECT run_id, status, wake_at, claim_token, claimed_at,
       poisoned_at, poison_reason, finished_at
FROM docket_runs
WHERE NOT (
  status IN ('running', 'waiting', 'done', 'failed', 'cancelled')
  AND ((status IN ('done', 'failed', 'cancelled')) = (finished_at IS NOT NULL))
  AND ((claim_token IS NULL) = (claimed_at IS NULL))
  AND ((poisoned_at IS NULL) = (poison_reason IS NULL))
  AND (status = 'running' OR
       (claim_token IS NULL AND wake_at IS NULL AND poisoned_at IS NULL))
  AND (poisoned_at IS NULL OR
       (status = 'running' AND claim_token IS NULL AND wake_at IS NULL))
  AND (status <> 'running' OR poisoned_at IS NOT NULL OR
       ((wake_at IS NOT NULL) <> (claim_token IS NOT NULL)))
  AND step >= 0
  AND checkpoint_seq >= 0
  AND claim_attempts >= 0
  AND claim_abandons >= 0
);

-- name: graph_references
SELECT graph_id, graph_hash, status, COUNT(*) AS retained_runs
FROM docket_runs
GROUP BY graph_id, graph_hash, status
ORDER BY graph_id, graph_hash, status;

-- name: retained_terminal_failures
SELECT run_id, tenant_id, graph_id, graph_hash, latest_checkpoint_type,
       checkpoint_seq, finished_at, updated_at
FROM docket_runs
WHERE status = 'failed'
ORDER BY finished_at DESC, id DESC;

-- name: retention_candidates
SELECT
  (SELECT COUNT(*)
   FROM docket_events
   WHERE inserted_at < CURRENT_TIMESTAMP - INTERVAL '30 days') AS old_events,
  (SELECT COUNT(*)
   FROM docket_runs
   WHERE status IN ('done', 'failed', 'cancelled')
     AND updated_at < CURRENT_TIMESTAMP - INTERVAL '90 days') AS old_terminal_runs;
