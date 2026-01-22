#!/bin/bash

set -e

# Prepare the database if it's not ready. No-op if the DB is prepared.
# In `rpc` mode, the node must not assume unique control over Postgres (no migrations / schema changes).
MODE="${EN_NODE_MODE:-}"
if [[ -z "$MODE" ]]; then
  # Best-effort parse of `--mode <value>` from args.
  for ((i=1; i<=$#; i++)); do
    if [[ "${!i}" == "--mode" ]]; then
      j=$((i+1))
      MODE="${!j}"
      break
    fi
  done
fi

if [[ "$MODE" != "rpc" ]]; then
  sqlx database setup
else
  echo "rpc mode: skipping 'sqlx database setup'"
fi
# Run the external node.
exec zksync_external_node "$@"
