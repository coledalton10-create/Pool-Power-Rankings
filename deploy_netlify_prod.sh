#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

./prepare_netlify_deploy.sh
netlify deploy --prod --dir=pages_site
