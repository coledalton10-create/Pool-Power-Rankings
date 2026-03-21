#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

./run_live_pool.sh

mkdir -p docs
rsync -a --delete --exclude='.git' pages_site/ docs/
touch docs/.nojekyll

echo "GitHub Pages bundle ready in docs/"
