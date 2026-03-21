#!/usr/bin/env python3
"""
Monte Carlo simulator for a 10-team NCAA tournament pool.

Inputs expected in the working directory:
1) monte_carlo_tournament_model_2026_mens_fixed_torvik_barthag_zscores.xlsx
2) pool_entries.csv

If `pool_entries.csv` is not present but `pool_entries.csv.xlsx` is, the script
will use the Excel file automatically.

Outputs:
1) pool_portfolio_results.csv
2) pool_simulation_detail.csv
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import math
import re
import sys

import numpy as np
import pandas as pd


TOURNAMENT_FILE = "monte_carlo_tournament_model_2026_mens_fixed_torvik_barthag_zscores.xlsx"
POOL_CSV_FILE = "pool_entries.csv"
POOL_XLSX_FILE = "pool_entries.csv.xlsx"
PORTFOLIO_OUTPUT_FILE = "pool_portfolio_results.csv"
DETAIL_OUTPUT_FILE = "pool_simulation_detail.csv"
DEFAULT_SIMULATIONS = 10_000


ROUND_DISPLAY_NAMES = {
    64: "Round 1",
    32: "Round 2",
    16: "Sweet 16",
    8: "Elite 8",
    4: "Final Four",
    2: "Championship",
}

# The workbook provides the regional bracket but does not explicitly list the
# national semifinal pairings, so we use the standard cross-region layout.
FINAL_FOUR_PAIRINGS = [("East", "West"), ("South", "Midwest")]

COMMON_TEAM_ALIASES = {
    "usf": ["South Florida"],
    "south florida": ["South Florida"],
    "miami": ["Miami (FL)"],
    "miami fl": ["Miami (FL)"],
    "miami florida": ["Miami (FL)"],
    "miami oh": ["Miami (OH)"],
    "miami ohio": ["Miami (OH)"],
    "ucf": ["UCF"],
    "central florida": ["UCF"],
    "mich st": ["Michigan State"],
    "mich state": ["Michigan State"],
    "michigan st": ["Michigan State"],
    "st johns": ["St. John's"],
    "saint johns": ["St. John's"],
    "st johns red storm": ["St. John's"],
    "saint marys": ["Saint Mary's"],
    "saint mary's": ["Saint Mary's"],
    "st marys": ["Saint Mary's"],
    "st mary's": ["Saint Mary's"],
    "texas nc st": ["NC State/Texas Winner"],
    "texas nc state": ["NC State/Texas Winner"],
    "nc st texas": ["NC State/Texas Winner"],
    "nc state texas": ["NC State/Texas Winner"],
    "unc": ["North Carolina"],
    "north carolina": ["North Carolina"],
    "ohio st": ["Ohio State"],
}

NON_AMBIGUOUS_ALIAS_KEYS = {
    "miami",
    "miami fl",
    "miami florida",
    "miami oh",
    "miami ohio",
}


@dataclass(frozen=True)
class TeamRecord:
    team_id: str
    team_name: str
    rating: float
    base_points: int


@dataclass(frozen=True)
class Game:
    game_id: str
    round_code: int
    region: str
    slot: int
    team1_id: str
    team2_id: str


def normalize_text(value: str) -> str:
    """Create a forgiving canonical form for team-name matching."""
    if value is None:
        return ""

    text = str(value).strip().lower()
    if not text:
        return ""

    replacements = {
        "&": " and ",
        "@": " at ",
        "st.": "saint ",
        "st ": "saint ",
        "mt.": "mount ",
        "mts.": "mount ",
        "a&m": "am",
        "'": "",
        "’": "",
        "`": "",
        ".": " ",
        ",": " ",
        "-": " ",
        "/": " ",
        "(": " ",
        ")": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"\buniversity\b", "", text)
    text = re.sub(r"\bcollege\b", "", text)
    text = re.sub(r"\bthe\b", "", text)
    text = re.sub(r"\bof\b", "", text)
    text = re.sub(r"\bnorth carolina\b", "unc", text)
    text = re.sub(r"\bconnecticut\b", "uconn", text)
    text = re.sub(r"\bsaint\b", "st", text)
    text = re.sub(r"\bstate\b", "st", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def variant_forms(name: str) -> List[str]:
    """Generate additional canonical variants for one team name."""
    normalized = normalize_text(name)
    variants = {normalized}
    if not normalized:
        return [normalized]

    variants.add(normalized.replace(" st ", " state "))
    variants.add(normalized.replace(" st ", " saint "))
    variants.add(normalized.replace(" and ", " "))
    variants.add(normalized.replace(" ", ""))

    tokens = normalized.split()
    if len(tokens) > 1:
        variants.add(" ".join(token for token in tokens if token not in {"st", "state", "saint"}))
        variants.add("".join(tokens))
        if tokens[0] == "north" and len(tokens) >= 2:
            variants.add("n " + " ".join(tokens[1:]))
        if tokens[0] == "south" and len(tokens) >= 2:
            variants.add("s " + " ".join(tokens[1:]))

    return [variant for variant in variants if variant]


def alias_candidates(name: str) -> List[str]:
    normalized = normalize_text(name)
    candidates: List[str] = []
    for alias in COMMON_TEAM_ALIASES.get(normalized, []):
        candidates.extend(variant_forms(alias))
    return [candidate for candidate in dict.fromkeys(candidates) if candidate]


def resolve_pool_entries_path(base_dir: Path) -> Path:
    csv_path = base_dir / POOL_CSV_FILE
    xlsx_path = base_dir / POOL_XLSX_FILE

    if csv_path.exists():
        return csv_path
    if xlsx_path.exists():
        return xlsx_path

    raise FileNotFoundError(
        f"Could not find either {POOL_CSV_FILE!r} or {POOL_XLSX_FILE!r} in {base_dir}"
    )


def read_pool_entries(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path, engine="openpyxl")

    df.columns = [str(column).strip() for column in df.columns]
    required = ["name"] + [f"team_{idx}" for idx in range(1, 11)]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Pool entries file is missing required columns: {missing}")

    return df


def read_workbook_sheets(path: Path) -> Dict[str, pd.DataFrame]:
    workbook = pd.ExcelFile(path, engine="openpyxl")
    return {
        sheet_name: workbook.parse(sheet_name, dtype=object)
        for sheet_name in workbook.sheet_names
    }


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [str(column).strip() for column in cleaned.columns]
    return cleaned


def pick_first_existing(columns: Iterable[str], candidates: Sequence[str]) -> str:
    lower_map = {str(column).strip().lower(): column for column in columns}
    for candidate in candidates:
        match = lower_map.get(candidate.lower())
        if match is not None:
            return match
    raise KeyError(f"Could not find any of these columns: {', '.join(candidates)}")


def compute_base_points(ap_rank: object) -> int:
    if pd.isna(ap_rank) or ap_rank == "":
        return 6

    ap_rank = int(ap_rank)
    if ap_rank <= 5:
        return 1
    if ap_rank <= 10:
        return 2
    if ap_rank <= 15:
        return 3
    if ap_rank <= 20:
        return 4
    if ap_rank <= 25:
        return 5
    return 6


def load_model_inputs(workbook_path: Path) -> Tuple[
    Dict[str, TeamRecord],
    Dict[str, Game],
    Dict[int, int],
    float,
    str,
]:
    sheets = read_workbook_sheets(workbook_path)

    teams_df = clean_columns(sheets["teams"]).copy()
    teams_df = teams_df.dropna(how="all")
    teams_df = teams_df[teams_df["team_id"].notna()].copy()
    teams_df["team_id"] = teams_df["team_id"].astype(str).str.strip()
    teams_df["team_name"] = teams_df["team_name"].astype(str).str.strip()

    rating_col = pick_first_existing(
        teams_df.columns,
        ["composite_rating", "rating", "composite", "team_rating"],
    )
    ap_col = pick_first_existing(
        teams_df.columns,
        ["ap_rank_week19", "ap_rank", "ap_rank_week_19"],
    )

    teams_df[rating_col] = pd.to_numeric(teams_df[rating_col], errors="coerce")
    teams_df[ap_col] = pd.to_numeric(teams_df[ap_col], errors="coerce")
    teams_df["base_points_computed"] = teams_df[ap_col].apply(compute_base_points)

    team_lookup: Dict[str, TeamRecord] = {}
    for row in teams_df.itertuples(index=False):
        if pd.isna(getattr(row, rating_col)):
            continue
        team_lookup[row.team_id] = TeamRecord(
            team_id=row.team_id,
            team_name=row.team_name,
            rating=float(getattr(row, rating_col)),
            base_points=int(row.base_points_computed),
        )

    first_round_df = clean_columns(sheets["first_round_matchups"]).copy()
    first_round_df = first_round_df.dropna(how="all")
    first_round_df = first_round_df[first_round_df["game_id"].notna()].copy()

    games: Dict[str, Game] = {}
    for row in first_round_df.itertuples(index=False):
        games[row.game_id] = Game(
            game_id=str(row.game_id).strip(),
            round_code=int(row.round_num),
            region=str(row.region).strip(),
            slot=int(row.slot),
            team1_id=str(row.team1_id).strip(),
            team2_id=str(row.team2_id).strip(),
        )

    scoring_df = clean_columns(sheets["scoring_rules"]).copy()
    scoring_df = scoring_df.dropna(how="all")
    scoring_df["round_code"] = pd.to_numeric(scoring_df["round_code"], errors="coerce")
    scoring_df["win_multiplier"] = pd.to_numeric(scoring_df["win_multiplier"], errors="coerce")
    scoring_df = scoring_df[
        scoring_df["round_code"].notna() & scoring_df["win_multiplier"].notna()
    ].copy()
    print("Cleaned scoring rules:")
    print(scoring_df[["round_name", "round_code", "win_multiplier"]].to_string(index=False))
    if scoring_df.empty:
        raise ValueError(
            "No valid scoring rows remain in the 'scoring_rules' sheet after filtering "
            "for numeric round_code and win_multiplier values."
        )
    round_multipliers = {
        int(row.round_code): int(row.win_multiplier)
        for row in scoring_df.itertuples(index=False)
    }

    params_df = clean_columns(sheets["params"]).copy()
    params_df = params_df.dropna(how="all")
    params_df["parameter"] = params_df["parameter"].astype(str).str.strip()
    params_df["value"] = pd.to_numeric(params_df["value"], errors="coerce")
    logistic_match = params_df.loc[params_df["parameter"] == "logistic_k", "value"]
    logistic_k = float(logistic_match.iloc[0]) if not logistic_match.empty else 1.0

    return team_lookup, games, round_multipliers, logistic_k, rating_col


def load_first_four_games(workbook_path: Path) -> Dict[str, Tuple[str, str]]:
    df = clean_columns(pd.read_excel(workbook_path, sheet_name="first_four_matchups", engine="openpyxl"))
    df = df.dropna(how="all")
    df = df[df["winner_placeholder_id"].notna()].copy()
    return {
        str(row.winner_placeholder_id).strip(): (
            str(row.team1_id).strip(),
            str(row.team2_id).strip(),
        )
        for row in df.itertuples(index=False)
    }


def build_name_mapping(team_lookup: Dict[str, TeamRecord]) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    for team in team_lookup.values():
        normalized = normalize_text(team.team_name)
        mapping.setdefault(normalized, [])
        if team.team_name not in mapping[normalized]:
            mapping[normalized].append(team.team_name)
    return mapping


def find_fuzzy_team_matches(raw_name: str, team_lookup: Dict[str, TeamRecord]) -> List[str]:
    normalized = normalize_text(raw_name)
    pool_tokens = set(normalized.split())
    if not pool_tokens:
        return []

    scored_matches: List[Tuple[int, int, str]] = []
    for team in team_lookup.values():
        best_score: Optional[Tuple[int, int, str]] = None
        for variant in variant_forms(team.team_name):
            team_tokens = set(variant.split())
            overlap = len(pool_tokens & team_tokens)
            if overlap == 0:
                continue
            penalty = abs(len(pool_tokens) - len(team_tokens))
            score = (overlap, -penalty, team.team_name)
            if best_score is None or score > best_score:
                best_score = score
        if best_score is not None:
            scored_matches.append(best_score)

    if not scored_matches:
        return []

    scored_matches.sort(reverse=True)
    best_overlap, best_penalty, _ = scored_matches[0]
    if best_overlap < max(1, len(pool_tokens) - 1):
        return []

    return sorted(
        {
            team_name
            for overlap, penalty, team_name in scored_matches
            if overlap == best_overlap and penalty == best_penalty
        }
    )


def find_ambiguous_team_matches(raw_name: str, team_lookup: Dict[str, TeamRecord]) -> List[str]:
    normalized = normalize_text(raw_name)
    exact_mapping = build_name_mapping(team_lookup)
    if exact_mapping.get(normalized):
        return []
    if normalized in NON_AMBIGUOUS_ALIAS_KEYS or normalized in COMMON_TEAM_ALIASES:
        return []
    return find_fuzzy_team_matches(raw_name, team_lookup)


def resolve_team_name(
    entry_value: object,
    team_lookup: Dict[str, TeamRecord],
    name_map: Dict[str, List[str]],
) -> Optional[str]:
    raw_name = "" if pd.isna(entry_value) else str(entry_value).strip()
    if not raw_name:
        raise ValueError("Encountered a blank selected team in the pool entries file.")

    normalized = normalize_text(raw_name)
    exact_matches = name_map.get(normalized, [])
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise ValueError(
            f"Ambiguous pool entry team name {raw_name!r}. "
            f"Possible workbook matches: {', '.join(sorted(exact_matches))}"
        )

    canonical_team_names = {team.team_name for team in team_lookup.values()}
    alias_matches = [team_name for team_name in COMMON_TEAM_ALIASES.get(normalized, []) if team_name in canonical_team_names]
    if len(alias_matches) == 1:
        return alias_matches[0]
    if len(alias_matches) > 1:
        raise ValueError(
            f"Ambiguous pool entry team name {raw_name!r}. "
            f"Possible workbook matches: {', '.join(sorted(alias_matches))}"
        )

    fuzzy_matches = find_fuzzy_team_matches(raw_name, team_lookup)
    if len(fuzzy_matches) == 1:
        return fuzzy_matches[0]
    if len(fuzzy_matches) > 1:
        raise ValueError(
            f"Ambiguous pool entry team name {raw_name!r}. "
            f"Possible workbook matches: {', '.join(fuzzy_matches)}"
        )

    return None


def match_team_name(entry_value: object, team_lookup: Dict[str, TeamRecord], name_map: Dict[str, List[str]]) -> str:
    raw_name = "" if pd.isna(entry_value) else str(entry_value).strip()
    match = resolve_team_name(entry_value, team_lookup, name_map)
    if match:
        return match

    candidates = [normalize_text(raw_name)] + alias_candidates(raw_name)

    raise ValueError(
        f"Could not match pool entry team name {raw_name!r}. "
        f"Tried normalized forms: {sorted(set(candidates))}"
    )


def report_unmatched_pool_team_names(entries_df: pd.DataFrame, team_lookup: Dict[str, TeamRecord]) -> List[str]:
    name_map = build_name_mapping(team_lookup)
    unmatched = set()
    for idx in range(1, 11):
        column = f"team_{idx}"
        for value in entries_df[column].dropna():
            if resolve_team_name(value, team_lookup, name_map) is None:
                unmatched.add(str(value).strip())

    print("Remaining unmatched pool team names:")
    if unmatched:
        for name in sorted(unmatched):
            print(f" - {name}")
    else:
        print(" - None")

    return sorted(unmatched)


def report_ambiguous_pool_team_names(entries_df: pd.DataFrame, team_lookup: Dict[str, TeamRecord]) -> Dict[str, List[str]]:
    ambiguous: Dict[str, List[str]] = {}
    for idx in range(1, 11):
        column = f"team_{idx}"
        for value in entries_df[column].dropna():
            raw_name = str(value).strip()
            matches = find_ambiguous_team_matches(raw_name, team_lookup)
            if len(matches) > 1:
                ambiguous[raw_name] = matches

    print("Ambiguous pool team names:")
    if ambiguous:
        for raw_name in sorted(ambiguous):
            print(f" - {raw_name}: {', '.join(ambiguous[raw_name])}")
    else:
        print(" - None")

    return ambiguous


def build_entries_matrix(entries_df: pd.DataFrame, team_lookup: Dict[str, TeamRecord]) -> Tuple[pd.DataFrame, np.ndarray, List[str]]:
    name_map = build_name_mapping(team_lookup)
    canonical_to_index = {
        team.team_name: idx for idx, team in enumerate(sorted(team_lookup.values(), key=lambda item: item.team_name))
    }
    ordered_team_names = [name for name, _ in sorted(canonical_to_index.items(), key=lambda item: item[1])]

    matrix_rows = []
    normalized_teams: List[List[str]] = []

    for row in entries_df.itertuples(index=False):
        chosen_names = [
            match_team_name(getattr(row, f"team_{idx}"), team_lookup, name_map)
            for idx in range(1, 11)
        ]
        normalized_teams.append(chosen_names)

        team_counts = np.zeros(len(ordered_team_names), dtype=np.int16)
        for team_name in chosen_names:
            team_counts[canonical_to_index[team_name]] += 1
        matrix_rows.append(team_counts)

    matrix = np.vstack(matrix_rows)
    entries_out = entries_df.copy()
    entries_out["teams"] = [", ".join(names) for names in normalized_teams]
    return entries_out, matrix, ordered_team_names


def logistic_win_probability(rating_a: float, rating_b: float, logistic_k: float) -> float:
    exponent = -logistic_k * (rating_a - rating_b)
    exponent = max(min(exponent, 700), -700)
    return 1.0 / (1.0 + math.exp(exponent))


def decide_game(team_a: TeamRecord, team_b: TeamRecord, rng: np.random.Generator, logistic_k: float) -> TeamRecord:
    probability_a = logistic_win_probability(team_a.rating, team_b.rating, logistic_k)
    return team_a if rng.random() < probability_a else team_b


def score_teams_by_round(round_winners: Dict[int, List[TeamRecord]], round_multipliers: Dict[int, int]) -> Dict[str, int]:
    team_scores: Dict[str, int] = {}
    for round_code, winners in round_winners.items():
        multiplier = round_multipliers[round_code]
        for team in winners:
            team_scores[team.team_name] = team_scores.get(team.team_name, 0) + team.base_points * multiplier
    return team_scores


def simulate_tournament(
    team_lookup: Dict[str, TeamRecord],
    first_four_lookup: Dict[str, Tuple[str, str]],
    first_round_games: Dict[str, Game],
    round_multipliers: Dict[int, int],
    logistic_k: float,
    rng: np.random.Generator,
) -> Dict[str, int]:
    placeholder_winners: Dict[str, TeamRecord] = {}
    for placeholder_id, (team1_id, team2_id) in first_four_lookup.items():
        team1 = team_lookup[team1_id]
        team2 = team_lookup[team2_id]
        placeholder_winners[placeholder_id] = decide_game(team1, team2, rng, logistic_k)

    regional_games: Dict[str, List[Game]] = {}
    for game in first_round_games.values():
        regional_games.setdefault(game.region, []).append(game)

    regional_winners_64: Dict[str, List[TeamRecord]] = {}
    round_winners: Dict[int, List[TeamRecord]] = {64: []}

    for region, games in regional_games.items():
        ordered_games = sorted(games, key=lambda game: game.slot)
        winners = []
        for game in ordered_games:
            team1 = placeholder_winners[game.team1_id] if game.team1_id in placeholder_winners else team_lookup[game.team1_id]
            team2 = placeholder_winners[game.team2_id] if game.team2_id in placeholder_winners else team_lookup[game.team2_id]
            winner = decide_game(team1, team2, rng, logistic_k)
            winners.append(winner)
            round_winners[64].append(winner)
        regional_winners_64[region] = winners

    current = regional_winners_64
    for round_code in (32, 16, 8):
        next_round: Dict[str, List[TeamRecord]] = {}
        round_winners[round_code] = []
        for region, teams in current.items():
            region_round_winners: List[TeamRecord] = []
            for idx in range(0, len(teams), 2):
                winner = decide_game(teams[idx], teams[idx + 1], rng, logistic_k)
                region_round_winners.append(winner)
                round_winners[round_code].append(winner)
            next_round[region] = region_round_winners
        current = next_round

    final_four_teams: Dict[str, TeamRecord] = {}
    for region, teams in current.items():
        if len(teams) != 1:
            raise ValueError(f"Region {region} should have exactly one team left before the Final Four.")
        final_four_teams[region] = teams[0]

    round_winners[4] = []
    championship_teams: List[TeamRecord] = []
    for region_a, region_b in FINAL_FOUR_PAIRINGS:
        winner = decide_game(final_four_teams[region_a], final_four_teams[region_b], rng, logistic_k)
        championship_teams.append(winner)
        round_winners[4].append(winner)

    round_winners[2] = [decide_game(championship_teams[0], championship_teams[1], rng, logistic_k)]
    return score_teams_by_round(round_winners, round_multipliers)


def calculate_finish_positions(scores: np.ndarray) -> np.ndarray:
    finishes = np.empty_like(scores, dtype=np.int16)
    for idx, score in enumerate(scores):
        finishes[idx] = 1 + int(np.sum(scores > score))
    return finishes


def summarize_results(
    entries_df: pd.DataFrame,
    score_matrix: np.ndarray,
    finish_matrix: np.ndarray,
) -> pd.DataFrame:
    results = pd.DataFrame({
        "name": entries_df["name"].astype(str),
        "avg_score": score_matrix.mean(axis=0),
        "median_score": np.median(score_matrix, axis=0),
        "p90_score": np.percentile(score_matrix, 90, axis=0),
        "std_dev": score_matrix.std(axis=0, ddof=0),
        "min_score": score_matrix.min(axis=0),
        "max_score": score_matrix.max(axis=0),
        "avg_finish": finish_matrix.mean(axis=0),
        "win_rate": (finish_matrix == 1).mean(axis=0),
        "top3_rate": (finish_matrix <= 3).mean(axis=0),
        "teams": entries_df["teams"],
    })
    return results.sort_values(["avg_score", "win_rate"], ascending=[False, False]).reset_index(drop=True)


def build_detail_output(
    entries_df: pd.DataFrame,
    score_matrix: np.ndarray,
    finish_matrix: np.ndarray,
) -> pd.DataFrame:
    names = entries_df["name"].astype(str).to_numpy()
    sim_ids = np.repeat(np.arange(1, score_matrix.shape[0] + 1), score_matrix.shape[1])
    repeated_names = np.tile(names, score_matrix.shape[0])
    return pd.DataFrame({
        "sim_id": sim_ids,
        "name": repeated_names,
        "score": score_matrix.reshape(-1),
        "finish_position": finish_matrix.reshape(-1),
    })


def print_leaderboards(summary_df: pd.DataFrame) -> None:
    print("\nTop 15 by avg_score")
    print(summary_df[["name", "avg_score", "win_rate", "teams"]].head(15).to_string(index=False))

    print("\nTop 15 by win_rate")
    print(
        summary_df.sort_values(["win_rate", "avg_score"], ascending=[False, False])[
            ["name", "win_rate", "avg_score", "teams"]
        ].head(15).to_string(index=False)
    )


def main() -> None:
    base_dir = Path.cwd()
    workbook_path = base_dir / TOURNAMENT_FILE
    if not workbook_path.exists():
        raise FileNotFoundError(f"Could not find tournament workbook: {workbook_path}")

    entries_path = resolve_pool_entries_path(base_dir)
    entries_df = read_pool_entries(entries_path)

    team_lookup, first_round_games, round_multipliers, logistic_k, rating_col = load_model_inputs(workbook_path)
    first_four_lookup = load_first_four_games(workbook_path)
    report_ambiguous_pool_team_names(entries_df, team_lookup)
    unmatched_names = report_unmatched_pool_team_names(entries_df, team_lookup)
    if unmatched_names:
        raise ValueError(
            "Pool entries contain unmatched team names after alias normalization: "
            + ", ".join(unmatched_names)
        )
    entries_df, entries_matrix, ordered_team_names = build_entries_matrix(entries_df, team_lookup)

    team_index = {team_name: idx for idx, team_name in enumerate(ordered_team_names)}
    rng = np.random.default_rng(20260319)

    score_matrix = np.zeros((DEFAULT_SIMULATIONS, len(entries_df)), dtype=np.int16)
    finish_matrix = np.zeros((DEFAULT_SIMULATIONS, len(entries_df)), dtype=np.int16)

    for sim_idx in range(DEFAULT_SIMULATIONS):
        team_scores = simulate_tournament(
            team_lookup=team_lookup,
            first_four_lookup=first_four_lookup,
            first_round_games=first_round_games,
            round_multipliers=round_multipliers,
            logistic_k=logistic_k,
            rng=rng,
        )

        score_vector = np.zeros(len(ordered_team_names), dtype=np.int16)
        for team_name, score in team_scores.items():
            score_vector[team_index[team_name]] = score

        entrant_scores = entries_matrix @ score_vector
        score_matrix[sim_idx] = entrant_scores
        finish_matrix[sim_idx] = calculate_finish_positions(entrant_scores)

    summary_df = summarize_results(entries_df, score_matrix, finish_matrix)
    detail_df = build_detail_output(entries_df, score_matrix, finish_matrix)

    summary_df.to_csv(base_dir / PORTFOLIO_OUTPUT_FILE, index=False)
    detail_df.to_csv(base_dir / DETAIL_OUTPUT_FILE, index=False)

    print(f"Read pool entries from: {entries_path.name}")
    print(f"Used team rating column: {rating_col}")
    print(f"Used logistic_k: {logistic_k}")
    print(f"Wrote summary output: {PORTFOLIO_OUTPUT_FILE}")
    print(f"Wrote detail output: {DETAIL_OUTPUT_FILE}")
    print_leaderboards(summary_df)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
