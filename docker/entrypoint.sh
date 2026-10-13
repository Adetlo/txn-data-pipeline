#!/bin/sh
# Wait for the metadata database (if PostgreSQL), apply migrations, then exec the command.
set -eu

if [ "${SKIP_MIGRATIONS:-0}" != "1" ]; then
  case "${METADATA_DATABASE_URL:-sqlite:///./data/metadata/pipeline.db}" in
    postgresql*)
      i=0
      until txn-pipeline db-upgrade >/dev/null 2>&1; do
        i=$((i + 1))
        if [ "$i" -ge 30 ]; then
          echo "metadata database not reachable after 30 attempts" >&2
          exit 1
        fi
        sleep 1
      done
      ;;
    *)
      txn-pipeline db-upgrade >/dev/null
      ;;
  esac
fi

exec "$@"
