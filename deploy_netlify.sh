#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="/Users/coledalton/march_madness_pool_model"
LOG_DIR="$SCRIPT_DIR/logs"
PREP_SCRIPT="$SCRIPT_DIR/prepare_netlify_deploy.sh"
NETLIFY_BIN="/Users/coledalton/.npm-global/bin/netlify"
NETLIFY_FALLBACK_JS="/Users/coledalton/.npm-global/lib/node_modules/netlify-cli/bin/run.js"

mkdir -p "$LOG_DIR"
cd "$SCRIPT_DIR"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PATH="/Users/coledalton/.npm-global/bin:$PATH"
export HOME="/Users/coledalton"
export CI="1"

echo "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] Starting deploy_netlify.sh from $SCRIPT_DIR"

if [ -x "$NETLIFY_BIN" ]; then
  echo "Using Netlify CLI at $NETLIFY_BIN"
elif [ -x "$NETLIFY_FALLBACK_JS" ] && command -v node >/dev/null 2>&1; then
  NETLIFY_BIN="$(command -v node) $NETLIFY_FALLBACK_JS"
  echo "Using Netlify CLI via node at $NETLIFY_BIN"
else
  echo "Error: Netlify CLI not found at $NETLIFY_BIN and fallback JS was unavailable." >&2
  exit 1
fi

"$PREP_SCRIPT"

deploy_output="$(eval "$NETLIFY_BIN" deploy --prod --dir="$SCRIPT_DIR/pages_site" 2>&1)"
deploy_status=$?
printf '%s\n' "$deploy_output"
if [ $deploy_status -ne 0 ]; then
  exit $deploy_status
fi

live_url="$(printf '%s\n' "$deploy_output" | awk -F': ' '/Website URL: /{print $2}' | tail -n 1)"
if [ -z "$live_url" ]; then
  live_url="$(printf '%s\n' "$deploy_output" | awk -F': ' '/Production URL: /{print $2}' | tail -n 1)"
fi
if [ -z "$live_url" ]; then
  live_url="$(printf '%s\n' "$deploy_output" | sed -n 's/.*Deployed to production URL: \(https:\/\/[^[:space:]]*\).*/\1/p' | tail -n 1)"
fi

if [ -n "$live_url" ]; then
  echo "Live site: $live_url"
fi

echo "Deploy command succeeded."
