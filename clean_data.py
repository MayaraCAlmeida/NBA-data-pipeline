
# NBA Data Pipeline - Data Cleaning Module

import os
import ssl
import logging
import pandas as pd
from glob import glob

ssl._create_default_https_context = ssl._create_unverified_context

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "dados_brutos")
PROCESSED_DIR = os.path.join(BASE_DIR, "dados_processados")
os.makedirs(PROCESSED_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# Helpers 

def latest_file(prefix: str) -> str:
    files = sorted(glob(os.path.join(RAW_DIR, f"{prefix}_*.csv")))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para o prefixo: {prefix}")
    return files[-1]


def save(df: pd.DataFrame, name: str) -> None:
    path = os.path.join(PROCESSED_DIR, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✔ {name}.csv salvo ({len(df)} linhas)")


# Funções de limpeza

def clean_player_gamelogs(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► player_gamelogs")

    df = df.rename(columns={
        "PLAYER_ID":       "player_id",
        "PLAYER_NAME":     "player_name",
        "TEAM_ID":         "team_id",
        "TEAM_ABBREVIATION": "team_abbr",
        "GAME_ID":         "game_id",
        "GAME_DATE":       "game_date",
        "MATCHUP":         "matchup",
        "WL":              "win_loss",
        "MIN":             "minutes",
        "FGM":             "fg_made",
        "FGA":             "fg_attempts",
        "FG_PCT":          "fg_pct",
        "FG3M":            "fg3_made",
        "FG3A":            "fg3_attempts",
        "FG3_PCT":         "fg3_pct",
        "FTM":             "ft_made",
        "FTA":             "ft_attempts",
        "FT_PCT":          "ft_pct",
        "OREB":            "off_rebounds",
        "DREB":            "def_rebounds",
        "REB":             "rebounds",
        "AST":             "assists",
        "TOV":             "turnovers",
        "STL":             "steals",
        "BLK":             "blocks",
        "BLKA":            "blocked_att",
        "PF":              "fouls",
        "PFD":             "fouls_drawn",
        "PTS":             "points",
        "PLUS_MINUS":      "plus_minus",
    })

    df["game_date"] = pd.to_datetime(df["game_date"])

    numeric_cols = [
        "minutes", "fg_made", "fg_attempts", "fg_pct",
        "fg3_made", "fg3_attempts", "fg3_pct",
        "ft_made", "ft_attempts", "ft_pct",
        "off_rebounds", "def_rebounds", "rebounds",
        "assists", "turnovers", "steals", "blocks",
        "points", "plus_minus",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["minutes"].notna() & (df["minutes"] > 0)]
    df["is_win"]  = (df["win_loss"] == "W").astype(int)
    df["is_home"] = df["matchup"].str.contains(r"vs\.").astype(int)
    df = df.drop_duplicates(subset=["player_id", "game_id"])

    log.info(f"  → {len(df)} linhas")
    return df


def clean_league_leaders(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► league_leaders")

    df = df.rename(columns={
        "PLAYER_ID":     "player_id",
        "PLAYER":        "player_name",
        "TEAM":          "team_abbr",
        "GP":            "games_played",
        "MIN":           "minutes_per_game",
        "FGM":           "fg_made",
        "FGA":           "fg_attempts",
        "FG_PCT":        "fg_pct",
        "FG3M":          "fg3_made",
        "FG3A":          "fg3_attempts",
        "FG3_PCT":       "fg3_pct",
        "FTM":           "ft_made",
        "FTA":           "ft_attempts",
        "FT_PCT":        "ft_pct",
        "OREB":          "off_rebounds",
        "DREB":          "def_rebounds",
        "REB":           "rebounds",
        "AST":           "assists",
        "TOV":           "turnovers",
        "STL":           "steals",
        "BLK":           "blocks",
        "PTS":           "points_per_game",
        "EFF":           "efficiency",
        "STAT_CATEGORY": "stat_category",
    })

    df = df.drop_duplicates(subset=["player_id", "stat_category"])

    log.info(f"  → {len(df)} linhas")
    return df


def clean_games(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► games")

    df = df.rename(columns={
        "TEAM_ID":           "team_id",
        "TEAM_ABBREVIATION": "team_abbr",
        "TEAM_NAME":         "team_name",
        "GAME_ID":           "game_id",
        "GAME_DATE":         "game_date",
        "MATCHUP":           "matchup",
        "WL":                "win_loss",
        "PTS":               "points",
        "PLUS_MINUS":        "point_diff",
    })

    df["game_date"] = pd.to_datetime(df["game_date"])
    df["is_home"] = df["matchup"].str.contains(r"vs\.").astype(int)
    df["is_win"]  = (df["win_loss"] == "W").astype(int)
    df = df.drop_duplicates(subset=["team_id", "game_id"])

    log.info(f"  → {len(df)} linhas")
    return df


def clean_players_meta(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► players_meta")

    df = df.rename(columns={
        "id":        "player_id",
        "full_name": "full_name",
        "first_name": "first_name",
        "last_name":  "last_name",
        "is_active":  "is_active",
    })

    df["is_active"] = df["is_active"].map({"True": True, "False": False}).fillna(False)
    df = df.drop_duplicates(subset=["player_id"])

    log.info(f"  → {len(df)} linhas")
    return df


def clean_teams_meta(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► teams_meta")

    df = df.rename(columns={
        "id":           "team_id",
        "full_name":    "full_name",
        "abbreviation": "abbreviation",
        "nickname":     "nickname",
        "city":         "city",
        "state":        "state",
        "year_founded": "year_founded",
    })

    df["year_founded"] = pd.to_numeric(df["year_founded"], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset=["team_id"])

    log.info(f"  → {len(df)} linhas")
    return df


# Main 

STEPS = {
    "player_gamelogs": clean_player_gamelogs,
    "league_leaders":  clean_league_leaders,
    "games":           clean_games,
    "players_meta":    clean_players_meta,
    "teams_meta":      clean_teams_meta,
}


def run():
    log.info(f"RAW → {RAW_DIR}")
    log.info(f"OUT → {PROCESSED_DIR}")

    ok = 0
    for name, fn in STEPS.items():
        try:
            path = latest_file(name)
            log.info(f"  Lendo: {os.path.basename(path)}")
            df = pd.read_csv(path)
            save(fn(df), name)
            ok += 1
        except Exception as e:
            log.error(f"  ✘ {name}: {e}")

    log.info(f"Concluído — {ok}/{len(STEPS)} etapas com sucesso.")


if __name__ == "__main__":
    run()
