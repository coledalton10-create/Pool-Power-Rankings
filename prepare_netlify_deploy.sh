#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

./run_live_pool.sh
cp pages_site/leaderboard.html pages_site/index.html

echo "Netlify bundle ready in pages_site/"
