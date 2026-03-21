#!/usr/bin/env python3
"""
Validation checks for the live March Madness pool pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from audit_current_scores import main as audit_current_scores_main
from live_pool_updater import (
    OUTPUT_CSV,
    OUTPUT_DASHBOARD_JSON,
    OUTPUT_PREVIOUS_CSV,
    OUTPUT_XLSX,
    SITE_OUTPUT_DIR,
    SNAPSHOT_STATE_FILE,
)


def load_status_sheet(workbook_path: Path) -> Dict[str, str]:
    status_df = pd.read_excel(workbook_path, sheet_name="Status", engine="openpyxl")
    return {
        str(row.metric): str(row.value)
        for row in status_df.itertuples(index=False)
    }


def compare_csv_files(path_a: Path, path_b: Path) -> bool:
    df_a = pd.read_csv(path_a).sort_values("name").reset_index(drop=True)
    df_b = pd.read_csv(path_b).sort_values("name").reset_index(drop=True)
    return df_a.equals(df_b)


def compare_json_files(path_a: Path, path_b: Path) -> bool:
    return json.loads(path_a.read_text(encoding="utf-8")) == json.loads(path_b.read_text(encoding="utf-8"))


def assert_close(label: str, value: float, target: float, tolerance: float, failures: List[str]) -> None:
    if abs(value - target) > tolerance:
        failures.append(
            f"{label} expected {target:.6f} +/- {tolerance:.6f}, got {value:.6f}"
        )


def validate_outputs(base_dir: Path) -> Tuple[Dict[str, object], List[str]]:
    failures: List[str] = []
    root_csv = base_dir / OUTPUT_CSV
    prev_csv = base_dir / OUTPUT_PREVIOUS_CSV
    root_xlsx = base_dir / OUTPUT_XLSX
    root_json = base_dir / OUTPUT_DASHBOARD_JSON
    root_state = base_dir / SNAPSHOT_STATE_FILE
    site_csv = base_dir / SITE_OUTPUT_DIR / OUTPUT_CSV
    site_prev_csv = base_dir / SITE_OUTPUT_DIR / OUTPUT_PREVIOUS_CSV
    site_json = base_dir / SITE_OUTPUT_DIR / OUTPUT_DASHBOARD_JSON
    site_state = base_dir / SITE_OUTPUT_DIR / SNAPSHOT_STATE_FILE

    required_paths = [root_csv, prev_csv, root_xlsx, root_json, root_state, site_csv, site_prev_csv, site_json, site_state]
    missing = [str(path.relative_to(base_dir)) for path in required_paths if not path.exists()]
    if missing:
        failures.append("Missing expected output files: " + ", ".join(missing))
        return {}, failures

    leaderboard = pd.read_csv(root_csv)
    status = load_status_sheet(root_xlsx)

    if not compare_csv_files(root_csv, site_csv):
        failures.append("Root live CSV and pages_site live CSV differ.")
    if not compare_csv_files(prev_csv, site_prev_csv):
        failures.append("Root previous CSV and pages_site previous CSV differ.")
    if not compare_json_files(root_json, site_json):
        failures.append("Root dashboard JSON and pages_site dashboard JSON differ.")
    if not compare_json_files(root_state, site_state):
        failures.append("Root snapshot state and pages_site snapshot state differ.")

    assert_close("win probability sum", float(leaderboard["live_win_rate"].sum()), 1.0, 0.02, failures)
    assert_close("top3 share sum", float(leaderboard["top3_rate"].sum()), 3.0, 0.05, failures)
    assert_close("top4 share sum", float(leaderboard["top4_rate"].sum()), 4.0, 0.05, failures)
    assert_close("last place share sum", float(leaderboard["last_rate"].sum()), 1.0, 0.02, failures)

    if leaderboard["remaining_live_teams"].min() < 0 or leaderboard["remaining_live_teams"].max() > 10:
        failures.append("remaining_live_teams fell outside the expected 0-10 range.")
    if leaderboard["best_case_score"].lt(leaderboard["current_score"]).any():
        failures.append("Found entries whose best_case_score is below current_score.")
    if leaderboard["worst_case_score"].lt(leaderboard["current_score"]).any():
        failures.append("Found entries whose worst_case_score is below current_score.")
    if leaderboard["best_case_score"].lt(leaderboard["worst_case_score"]).any():
        failures.append("Found entries whose best_case_score is below worst_case_score.")
    if (leaderboard["current_rank"] > len(leaderboard)).any():
        failures.append("Found invalid current ranks in live output.")
    required_columns = {"trend_rank_delta", "trend_score_delta", "trend_odds_delta", "top4_rate"}
    missing_columns = sorted(required_columns - set(leaderboard.columns))
    if missing_columns:
        failures.append("Live leaderboard is missing required columns: " + ", ".join(missing_columns))

    dashboard_payload = json.loads(root_json.read_text(encoding="utf-8"))
    snapshot_state = json.loads(root_state.read_text(encoding="utf-8"))
    if len(dashboard_payload.get("rows", [])) != len(leaderboard):
        failures.append("Dashboard JSON row count does not match live CSV row count.")
    if any("teams" not in row for row in dashboard_payload.get("rows", [])):
        failures.append("Dashboard JSON rows are missing team-status payloads.")
    if snapshot_state.get("current_snapshot_key") != status.get("current_snapshot_key"):
        failures.append("Snapshot state current key does not match workbook status sheet.")
    if str(snapshot_state.get("trend_baseline_updated_this_run")).lower() != str(status.get("trend_baseline_updated_this_run", "")).lower():
        failures.append("Snapshot baseline-updated flag does not match workbook status sheet.")

    diagnostics = {
        "entry_count": int(len(leaderboard)),
        "completed_games": int(float(status["completed_games"])),
        "live_games": int(float(status["live_games"])),
        "remaining_games_with_odds": int(float(status["remaining_games_with_odds"])),
        "remaining_games_using_fallback_model": int(float(status["remaining_games_using_fallback_model"])),
        "simulations_run": int(float(status["simulations_run"])),
        "win_probability_sum": float(leaderboard["live_win_rate"].sum()),
        "top3_share_sum": float(leaderboard["top3_rate"].sum()),
        "top4_share_sum": float(leaderboard["top4_rate"].sum()),
        "last_place_share_sum": float(leaderboard["last_rate"].sum()),
        "trend_baseline_updated_this_run": bool(snapshot_state.get("trend_baseline_updated_this_run")),
        "current_snapshot_key": str(snapshot_state.get("current_snapshot_key") or ""),
        "previous_snapshot_key": str(snapshot_state.get("previous_snapshot_key") or ""),
        "output_files": [
            str(root_csv.relative_to(base_dir)),
            str(prev_csv.relative_to(base_dir)),
            str(root_xlsx.relative_to(base_dir)),
            str(root_json.relative_to(base_dir)),
            str(root_state.relative_to(base_dir)),
            str(site_csv.relative_to(base_dir)),
            str(site_prev_csv.relative_to(base_dir)),
            str(site_json.relative_to(base_dir)),
            str(site_state.relative_to(base_dir)),
        ],
    }
    return diagnostics, failures


def main() -> None:
    base_dir = Path.cwd()

    print("Recomputing current scores against saved winners...")
    audit_current_scores_main()

    diagnostics, failures = validate_outputs(base_dir)
    diagnostics_path = base_dir / "live_pool_diagnostics.json"
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")

    print("\nValidation summary")
    print("==================")
    for key, value in diagnostics.items():
        print(f"{key}: {value}")
    print(f"diagnostics_file: {diagnostics_path.name}")

    if failures:
        print("\nValidation failures")
        print("===================")
        for failure in failures:
            print(f" - {failure}")
        raise SystemExit(1)

    print("\nAll validation checks passed.")


if __name__ == "__main__":
    main()
