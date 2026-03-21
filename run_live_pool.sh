#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export NCAA_SCOREBOARD_DAYS="${NCAA_SCOREBOARD_DAYS:-2}"
export NCAA_SCOREBOARD_BASE_DATE="${NCAA_SCOREBOARD_BASE_DATE:-$(TZ=America/New_York date +%F)}"
export POOL_SIMULATIONS="${POOL_SIMULATIONS:-10000}"

python3 live_pool_updater.py
python3 validate_pool_pipeline.py
cp pages_site/leaderboard.html pages_site/index.html
