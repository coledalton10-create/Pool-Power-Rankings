# March Madness Live Pool Odds

This repo's canonical live pipeline is the `run_live_pool.sh` command at the project root.

## Canonical Workflow / Project Status

- One rerun command: `./run_live_pool.sh`
- One diagnostics command: `python3 validate_pool_pipeline.py`
- Canonical scripts:
  - `run_live_pool.sh`
  - `live_pool_updater.py`
  - `validate_pool_pipeline.py`
- Canonical visible-output wiring:
  - `pages_site/leaderboard.html` reads `pages_site/live_pool_odds.csv`
  - root `leaderboard.html` reads root `live_pool_odds.csv`
- Canonical outputs:
  - `live_pool_odds.csv`
  - `live_pool_odds_previous.csv`
  - `live_pool_odds.xlsx`
  - `live_pool_dashboard.json`
  - `live_pool_snapshot_state.json`
  - `pages_site/live_pool_odds.csv`
  - `pages_site/live_pool_odds_previous.csv`
  - `pages_site/live_pool_dashboard.json`
  - `pages_site/live_pool_snapshot_state.json`
- `live_pool_diagnostics.json`
- Legacy files that should not be relied on for live updates:
  - `simulate_pool_portfolio.py`
  - `pool_portfolio_results.csv`
  - `pool_portfolio_results_master.csv`
  - `pool_portfolio_results_master.xlsx`
  - `pool_portfolio_results_enhanced.csv`
  - `pool_simulation_detail.csv`

## Canonical inputs

- `pool_entries.csv.xlsx` or `pool_entries.csv`: pool entry picks and tiebreakers
- `monte_carlo_tournament_model_2026_mens_fixed_torvik_barthag_zscores.xlsx`: bracket structure, team ratings, and scoring rules
- `resolved_winners.json`: locally persisted completed-game winners
- `.env`: contains `THE_ODDS_API_KEY`

## Canonical run command

```bash
./run_live_pool.sh
```

What it does:

1. Loads entries, scoring rules, and bracket structure.
2. Pulls current NCAA scoreboard state and The Odds API prices when available.
3. Locks completed games, simulates only the remaining bracket, and splits first-place ties fairly.
4. Regenerates the root outputs and mirrors the CSVs into `pages_site/` so the HTML dashboard stays in sync.
5. Runs validation checks and writes `live_pool_diagnostics.json`.

## Canonical outputs

- `live_pool_odds.csv`: canonical machine-readable live leaderboard and win odds
- `live_pool_odds_previous.csv`: prior snapshot for trend arrows
- `live_pool_odds.xlsx`: human-readable workbook with status, game state, and leaderboard tabs
- `live_pool_dashboard.json`: canonical dashboard payload for the hostable front-end
- `live_pool_snapshot_state.json`: snapshot metadata used to control completed-game-only trend rollovers
- `pages_site/live_pool_odds.csv`: mirrored site data for the static dashboard
- `pages_site/live_pool_odds_previous.csv`: mirrored prior site snapshot
- `pages_site/live_pool_dashboard.json`: mirrored site dashboard payload
- `pages_site/live_pool_snapshot_state.json`: mirrored site snapshot metadata
- `live_pool_diagnostics.json`: validation summary for the latest run

## Validation-only command

```bash
python3 validate_pool_pipeline.py
```

This recomputes current scores from saved winners, checks output presence, verifies root/site CSV parity, and confirms fair-share probability totals are sane.

## Trend snapshot logic

- Trends compare the current snapshot against the prior completed-game snapshot only.
- `live_pool_odds_previous.csv` is not rolled forward on every refresh.
- The previous snapshot advances only when the completed-game snapshot key changes.
- In-progress-game refreshes leave the trend baseline untouched.
- Snapshot metadata is written to `live_pool_snapshot_state.json` and mirrored into `pages_site/`.

## Hosting / publish flow

- Canonical static source bundle: `pages_site/`
- GitHub Pages publish mirror: `docs/`
- The front-end reads only `live_pool_dashboard.json` inside the hosted folder
- Recommended one-command GitHub Pages deploy flow: `./deploy_github_pages.sh`

Update + publish flow:

1. Run `./deploy_github_pages.sh`
2. Verify `live_pool_diagnostics.json` if the script reports a newly generated publish
3. GitHub Pages serves the pushed `docs/` folder from your default branch

## GitHub Pages setup

- Rerun + publish command: `./deploy_github_pages.sh`
- Folder that GitHub Pages should publish: `docs/`
- Source bundle that should remain canonical locally: `pages_site/`

One-time GitHub steps:

1. Push this repo to GitHub.
2. Open the repo on GitHub.
3. Go to `Settings` -> `Pages`.
4. Under `Build and deployment`, choose `Deploy from a branch`.
5. Select your default branch.
6. Select the `/docs` folder.
7. Click `Save`.

Future updates:

1. Run `./deploy_github_pages.sh`
2. Wait for GitHub Pages to republish the site

## Legacy optional deploy scripts

- `./prepare_netlify_deploy.sh`
- `./deploy_netlify.sh`
- `com.coledalton.marchmadnesspool.netlify-deploy.plist`

These are retained as legacy Netlify helpers and are no longer the recommended or scheduled publish workflow.

## Notes

- If the NCAA scoreboard or The Odds API is unavailable, the updater falls back gracefully:
  - completed games still come from `resolved_winners.json`
  - remaining games without live market prices use the workbook rating model
- Pool-entry `tiebreaker` values are present, but this repo does not contain enough information to simulate the real tiebreaker outcome. First-place ties are therefore split evenly in the live win odds.
- The canonical hostable dashboard source is the static site in `pages_site/`; `docs/` is a GitHub Pages publish mirror and the root `leaderboard.html` is only a local mirror.
- `simulate_pool_portfolio.py` and the `pool_portfolio_results*` files are legacy pre-tournament artifacts, not the canonical live workflow.
