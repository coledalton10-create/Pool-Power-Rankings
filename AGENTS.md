# AGENTS.md

## Purpose

This repo is a March Madness pool leaderboard and odds-to-win product. Work autonomously and use the full context window well. Preserve momentum, avoid re-deriving settled decisions, and optimize for a trustworthy working system over theoretical discussion.

## Default Operating Mode

- Act like the lead full-stack/data engineer for this repo.
- Do not ask for tiny-step confirmation.
- Inspect, patch, verify, and hand off cleanly.
- Keep moving until the task is actually finished or you hit a real blocker.
- Stop only for true blockers such as:
  - missing required input files
  - missing auth or external access with no reasonable fallback
  - a required irreversible/destructive action
  - a genuinely ambiguous product decision that cannot be inferred from the repo

## Repo Rules

- Preserve the canonical data pipeline.
- `run_live_pool.sh` is the canonical rerun entry point.
- `live_pool_updater.py` is the canonical scoring/simulation/output pipeline.
- `validate_pool_pipeline.py` is the canonical diagnostics/validation path.
- `pages_site/` is the canonical static host bundle.
- `docs/` is the GitHub Pages publish mirror.
- Prefer static-hosting-first solutions.
- Do not change dashboard data semantics unless explicitly asked.
- Trustworthiness of pool data matters more than elegance or cleverness.
- Do not leave multiple competing output paths unresolved.
- Do not let visible dashboard data drift from canonical outputs.

## Working Style

- Audit first when needed, but do not restart from scratch without evidence.
- Prefer small, grounded fixes over broad refactors unless a broader change is clearly necessary.
- Verify meaningful claims with actual checks.
- If you change outputs, rerun the relevant flow and confirm the host bundle reflects the new data.
- Keep legacy helpers isolated rather than deleting aggressively unless clearly safe.

## Handoff Requirements

Every final handoff should include:

1. what changed
2. what was verified
3. the exact command to run
4. remaining manual steps
5. blockers or risks
