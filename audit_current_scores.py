#!/usr/bin/env python3
"""
Local-only audit for current score calculation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from live_pool_updater import (
    RESOLVED_WINNERS_FILE,
    TOURNAMENT_FILE,
    build_bracket_template,
    compute_current_state,
    load_env_file,
    load_resolved_winners,
)
from simulate_pool_portfolio import (
    build_entries_matrix,
    load_model_inputs,
    read_pool_entries,
    resolve_pool_entries_path,
)


def compute_entry_breakdowns(
    entries_df: pd.DataFrame,
    team_scores: Dict[str, int],
    team_lookup_by_name,
    round_winner_points: Dict[str, List[tuple]],
) -> Dict[str, List[tuple]]:
    breakdowns: Dict[str, List[tuple]] = {}
    for row in entries_df.itertuples(index=False):
        name = str(row.name)
        chosen_teams = [team.strip() for team in str(row.teams).split(",")]
        contributions = []
        for team_name in chosen_teams:
            if team_name in round_winner_points:
                for base_points, multiplier, contributed in round_winner_points[team_name]:
                    contributions.append((team_name, base_points, multiplier, contributed))
        breakdowns[name] = contributions
    return breakdowns


def main() -> None:
    base_dir = Path.cwd()
    load_env_file(base_dir / ".env")

    workbook_path = base_dir / TOURNAMENT_FILE
    entries_path = resolve_pool_entries_path(base_dir)
    resolved_winners_path = base_dir / RESOLVED_WINNERS_FILE

    entries_df = read_pool_entries(entries_path)
    team_lookup, _games, round_multipliers, _logistic_k, _rating_col = load_model_inputs(workbook_path)
    team_lookup_by_name = {team.team_name: team for team in team_lookup.values()}
    entries_df, entries_matrix, ordered_team_names = build_entries_matrix(entries_df, team_lookup)
    team_index = {team_name: idx for idx, team_name in enumerate(ordered_team_names)}

    saved_winners = load_resolved_winners(resolved_winners_path)
    nodes = build_bracket_template(workbook_path, saved_winners)
    live_state = {node_id: None for node_id in nodes}
    from live_pool_updater import LiveNodeState  # local import to avoid changing updater logic
    live_state = {
        node_id: LiveNodeState(game=node, fixed_winner=saved_winners.get(node_id))
        for node_id, node in nodes.items()
    }

    current_team_scores, _eliminated_teams = compute_current_state(
        nodes,
        live_state,
        team_lookup_by_name,
        round_multipliers,
    )

    team_score_vector = np.zeros(len(ordered_team_names), dtype=np.int16)
    for team_name, score in current_team_scores.items():
        team_score_vector[team_index[team_name]] = score
    recomputed_scores = entries_matrix @ team_score_vector

    workbook_results_path = base_dir / "live_pool_odds.csv"
    if not workbook_results_path.exists():
        raise FileNotFoundError("live_pool_odds.csv not found; run the updater first to compare workbook current scores.")
    workbook_results = pd.read_csv(workbook_results_path)
    workbook_score_map = dict(zip(workbook_results["name"].astype(str), workbook_results["current_score"]))

    round_winner_points: Dict[str, List[tuple]] = {}
    for node_id, winner in saved_winners.items():
        node = nodes.get(node_id)
        if node is None or node.round_code not in round_multipliers or node.round_code < 64:
            continue
        base_points = team_lookup_by_name[winner].base_points
        multiplier = round_multipliers[node.round_code]
        contributed = base_points * multiplier
        round_winner_points.setdefault(winner, []).append((base_points, multiplier, contributed))

    breakdowns = compute_entry_breakdowns(entries_df, current_team_scores, team_lookup_by_name, round_winner_points)

    rows = []
    mismatches = []
    for row, recomputed in zip(entries_df.itertuples(index=False), recomputed_scores):
        name = str(row.name)
        workbook_score = workbook_score_map.get(name)
        match = workbook_score == recomputed
        rows.append((name, workbook_score, int(recomputed), "Match" if match else "Mismatch"))
        if not match:
            mismatches.append(name)

    audit_df = pd.DataFrame(rows, columns=["Name", "Workbook Current Score", "Recomputed Current Score", "Match / Mismatch"])
    print(audit_df.to_string(index=False))

    for name in mismatches:
        print(f"\nMismatch breakdown for {name}")
        total = 0
        for team_name, base_points, multiplier, contributed in breakdowns.get(name, []):
            total += contributed
            print(
                f" - {team_name}: base_points={base_points}, "
                f"round_multiplier={multiplier}, contributed={contributed}"
            )
        print(f" Recomputed total: {total}")
        print(f" Workbook total: {workbook_score_map.get(name)}")


if __name__ == "__main__":
    main()
