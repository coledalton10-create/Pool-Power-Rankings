This folder is the canonical hostable static dashboard bundle.

For GitHub Pages, mirror this folder into `../docs/` with `./prepare_github_pages_publish.sh`.
The primary publish command is `./deploy_github_pages.sh`, which refreshes this bundle, mirrors it into `docs/`, and pushes the updated `docs/` publish mirror.

Primary files:

- `index.html`
- `leaderboard.html`
- `live_pool_dashboard.json`
- `live_pool_odds.csv`
- `live_pool_odds_previous.csv`
- `live_pool_snapshot_state.json`
