#!/usr/bin/env python3
"""
Diagnostic-only audit for team-name matching across:
1) internal bracket/model names
2) NCAA scoreboard names
3) The Odds API odds names
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Set

from live_pool_updater import (
    TOURNAMENT_FILE,
    build_team_id_index,
    build_team_name_index,
    canonicalize_name,
    load_env_file,
    load_ncaa_scoreboard_games,
    load_the_odds_api_odds,
    normalize_team_key,
)
from simulate_pool_portfolio import load_model_inputs


def collect_ncaa_names(score_events: List[dict]) -> List[str]:
    names: Set[str] = set()
    for event in score_events:
        for score in event.get("scores", []):
            name = str(score.get("name") or "").strip()
            if name:
                names.add(name)
        for key in ("away_team", "home_team"):
            name = str(event.get(key) or "").strip()
            if name:
                names.add(name)
    return sorted(names)


def collect_odds_names(odds_events: List[dict]) -> List[str]:
    names: Set[str] = set()
    for event in odds_events:
        for key in ("away_team", "home_team"):
            name = str(event.get(key) or "").strip()
            if name:
                names.add(name)
        for bookmaker in event.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                for outcome in market.get("outcomes", []):
                    name = str(outcome.get("name") or "").strip()
                    if name:
                        names.add(name)
    return sorted(names)


def print_mapping_report(
    source_label: str,
    raw_names: Iterable[str],
    internal_names: Set[str],
    internal_normalized: Dict[str, str],
    team_name_index: Dict[str, str],
    team_id_index: Dict[str, str],
) -> None:
    exact_matches: List[str] = []
    normalized_matches: List[str] = []
    unresolved: List[str] = []

    for raw_name in sorted(set(raw_names)):
        canonical = canonicalize_name(raw_name, team_name_index, team_id_index)
        if raw_name in internal_names:
            exact_matches.append(raw_name)
        elif canonical:
            normalized_matches.append(f"{raw_name} -> {canonical}")
        else:
            unresolved.append(raw_name)

    print(f"\n{source_label} exact matches")
    if exact_matches:
        for name in exact_matches:
            print(f" - {name}")
    else:
        print(" - None")

    print(f"\n{source_label} normalized matches")
    if normalized_matches:
        for item in normalized_matches:
            print(f" - {item}")
    else:
        print(" - None")

    print(f"\n{source_label} unresolved names")
    if unresolved:
        for name in unresolved:
            print(f" - {name}")
    else:
        print(" - None")

    print(f"\n{source_label} suggested aliases")
    if unresolved:
        for name in unresolved:
            normalized = normalize_team_key(name)
            suggestion = internal_normalized.get(normalized)
            if suggestion:
                print(f" - {normalized!r}: {suggestion!r}")
            else:
                print(f" - {normalized!r}: '<add canonical team>'")
    else:
        print(" - None")


def main() -> None:
    base_dir = Path.cwd()
    load_env_file(base_dir / ".env")

    workbook_path = base_dir / TOURNAMENT_FILE
    team_lookup, _games, _round_multipliers, _logistic_k, _rating_col = load_model_inputs(workbook_path)
    internal_names = {team.team_name for team in team_lookup.values()}
    internal_normalized = {normalize_team_key(name): name for name in internal_names}
    team_name_index = build_team_name_index(team_lookup)
    team_id_index = build_team_id_index(team_lookup)

    print("Internal team names")
    for name in sorted(internal_names):
        print(f" - {name}")

    score_events, _ = load_ncaa_scoreboard_games()
    odds_events, _ = load_the_odds_api_odds(score_events)

    print_mapping_report(
        "NCAA scoreboard",
        collect_ncaa_names(score_events),
        internal_names,
        internal_normalized,
        team_name_index,
        team_id_index,
    )
    print_mapping_report(
        "The Odds API",
        collect_odds_names(odds_events),
        internal_names,
        internal_normalized,
        team_name_index,
        team_id_index,
    )


if __name__ == "__main__":
    main()
