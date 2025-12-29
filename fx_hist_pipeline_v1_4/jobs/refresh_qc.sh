#!/usr/bin/env bash
set -euo pipefail
DB=${1:-fx}
psql -d "$DB" -v ON_ERROR_STOP=1 -c "SELECT refresh_fx_session_qc_materialized();"
echo "QC materialized views refreshed."
