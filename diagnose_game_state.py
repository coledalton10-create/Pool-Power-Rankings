#!/usr/bin/env python3
"""
Targeted local-only diagnostic for specific bracket nodes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from live_pool_updater import (
    TOURNAMENT_FILE,
    RESOLVED_WINNERS_FILE,
    build_bracket_template,
    build_team_id_index,
    build_team_name_index,
    load_env_file,
    load_resolved_winners,
    resolve_source,
)
from simulate_pool_portfolio import load_model_inputs


def print_node(node_id: str, nodes, saved_winners, team_name_index, team_id_index) -> None:
    node = nodes[node_id]
    resolved_team_a = resolve_source(node.source_a, saved_winners, team_name_index, team_id_index)
    resolved_team_b = resolve_source(node.source_b, saved_winners, team_name_index, team_id_index)

    print(f"\nnode_id: {node.node_id}")
    print(f"round_code: {node.round_code}")
    print(f"region: {node.region}")
    print(f"slot: {node.slot}")
    print(f"source_a: {node.source_a}")
    print(f"source_b: {node.source_b}")
    print(f"resolved team_a: {resolved_team_a}")
    print(f"resolved team_b: {resolved_team_b}")


def find_round64_node(nodes, *, team_a: Optional[str] = None, team_b: Optional[str] = None, contains_team: Optional[str] = None):
    for node in nodes.values():
        if node.round_code != 64:
            continue
        source_a = str(node.source_a)
        source_b = str(node.source_b)
        if team_a and team_b and {source_a, source_b} == {team_a, team_b}:
            return node.node_id
        if contains_team and contains_team in {source_a, source_b}:
            return node.node_id
    raise KeyError(
        f"Could not find Round of 64 node for criteria: "
        f"team_a={team_a}, team_b={team_b}, contains_team={contains_team}"
    )


def main() -> None:
    base_dir = Path.cwd()
    load_env_file(base_dir / ".env")

    workbook_path = base_dir / TOURNAMENT_FILE
    resolved_winners_path = base_dir / RESOLVED_WINNERS_FILE

    team_lookup, _games, _round_multipliers, _logistic_k, _rating_col = load_model_inputs(workbook_path)
    team_name_index = build_team_name_index(team_lookup)
    team_id_index = build_team_id_index(team_lookup)
    nodes = build_bracket_template(workbook_path)
    saved_winners = load_resolved_winners(resolved_winners_path)

    requested = [
        "FF_M_11",
        "FF_M_16",
        "FF_S_16",
        "FF_W_11",
        "R64_W_05",
        find_round64_node(nodes, team_a="MICH", team_b="FF_M_16"),
        find_round64_node(nodes, contains_team="BYU"),
    ]

    for node_id in requested:
        print_node(node_id, nodes, saved_winners, team_name_index, team_id_index)


if __name__ == "__main__":
    main()
