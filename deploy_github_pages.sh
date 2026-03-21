#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="/Users/coledalton/march_madness_pool_model"
LOG_DIR="$SCRIPT_DIR/logs"
PREP_SCRIPT="$SCRIPT_DIR/prepare_github_pages_publish.sh"

PUBLISH_PATHS=(
  "docs"
  "pages_site/index.html"
  "pages_site/leaderboard.html"
  "pages_site/live_pool_dashboard.json"
  "pages_site/live_pool_odds.csv"
  "pages_site/live_pool_odds_previous.csv"
  "pages_site/live_pool_snapshot_state.json"
  "live_pool_dashboard.json"
  "live_pool_diagnostics.json"
  "live_pool_odds.csv"
  "live_pool_odds.xlsx"
  "live_pool_odds_previous.csv"
  "live_pool_snapshot_state.json"
)

mkdir -p "$LOG_DIR"
cd "$SCRIPT_DIR"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export HOME="/Users/coledalton"
export CI="1"

echo "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] Starting deploy_github_pages.sh from $SCRIPT_DIR"

"$PREP_SCRIPT"

if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
  echo "Error: $SCRIPT_DIR is not inside a git worktree." >&2
  echo "Operator handoff: run this deploy from the GitHub-backed repository checkout that will publish /docs." >&2
  exit 1
fi

status_output="$(git status --porcelain=v1 --untracked-files=normal -- "${PUBLISH_PATHS[@]}")"
if [ -z "$status_output" ]; then
  echo "No tracked publish-relevant changes detected. Nothing to commit or push."
  echo "Operator handoff: docs/ already matches pages_site/ and origin does not need a new publish."
  exit 0
fi

printf '%s\n' "$status_output"

git add -- "${PUBLISH_PATHS[@]}"

if git diff --cached --quiet -- "${PUBLISH_PATHS[@]}"; then
  echo "No staged publish-relevant delta remained after git add. Nothing to commit or push."
  echo "Operator handoff: publish inputs are already committed."
  exit 0
fi

if ! git remote get-url origin >/dev/null 2>&1; then
  echo "Error: git remote 'origin' is not configured." >&2
  echo "Operator handoff: add the GitHub remote, then rerun ./deploy_github_pages.sh." >&2
  exit 1
fi

branch_name="$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
if [ -z "$branch_name" ]; then
  branch_name="$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null | sed 's@^origin/@@')"
fi

if [ -z "$branch_name" ]; then
  echo "Error: could not determine the current or default branch for origin." >&2
  echo "Operator handoff: check out the branch GitHub Pages should publish from, then rerun ./deploy_github_pages.sh." >&2
  exit 1
fi

commit_message="Auto-publish GitHub Pages bundle $(/bin/date '+%Y-%m-%d %H:%M:%S %Z')"
git commit -m "$commit_message"
git push origin "$branch_name"

echo "Pushed publish refresh to origin/$branch_name."
echo "Operator handoff: GitHub Pages will publish the updated docs/ folder from origin/$branch_name."
