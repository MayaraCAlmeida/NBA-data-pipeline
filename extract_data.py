"""
NBA Data Pipeline - Data Extraction Module

"""

import os
import ssl
import time
import logging
import pandas as pd
from datetime import datetime

# Fix SSL em alguns ambientes (Windows/Mac)
ssl._create_default_https_context = ssl._create_unverified_context

from nba_api.stats.endpoints import (
    leaguegamefinder,
    playergamelogs,
    leagueleaders,
)
from nba_api.stats.static import players, teams

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "dados_brutos")
os.makedirs(RAW_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

SEASON = "2024-25"
SEASON_TYPE = "Regular Season"
DELAY = 1.0  # segundos entre requests (respeita rate limit)


# ─── Helpers ─────────────────────────────────────────────────────────────────
def save_raw(df: pd.DataFrame, name: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d")
    path = os.path.join(RAW_DIR, f"{name}_{timestamp}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✔ Salvo: {os.path.basename(path)}  ({len(df)} linhas)")
    return path


def safe_request(func, *args, retries=3, **kwargs):
    """Chama endpoints da nba_api com retry automático."""
    for attempt in range(1, retries + 1):
        try:
            result = func(*args, **kwargs)
            time.sleep(DELAY)
            return result
        except Exception as e:
            log.warning(f"  Tentativa {attempt}/{retries} falhou: {e}")
            time.sleep(DELAY * attempt * 2)
    raise RuntimeError(f"Falhou após {retries} tentativas")


# ─── Funções de extração ──────────────────────────────────────────────────────
def extract_players_meta() -> pd.DataFrame:
    log.info("► Jogadores ativos (metadata)...")
    df = pd.DataFrame(players.get_active_players())
    save_raw(df, "players_meta")
    return df


def extract_teams_meta() -> pd.DataFrame:
    log.info("► Times da NBA (metadata)...")
    df = pd.DataFrame(teams.get_teams())
    save_raw(df, "teams_meta")
    return df


def extract_games() -> pd.DataFrame:
    log.info("► Jogos da temporada...")
    endpoint = safe_request(
        leaguegamefinder.LeagueGameFinder,
        season_nullable=SEASON,
        league_id_nullable="00",
        season_type_nullable=SEASON_TYPE,
    )
    df = endpoint.get_data_frames()[0]
    save_raw(df, "games")
    return df


def extract_player_gamelogs() -> pd.DataFrame:
    log.info("► Game logs dos jogadores (pode demorar ~1-2 min)...")
    endpoint = safe_request(
        playergamelogs.PlayerGameLogs,
        season_nullable=SEASON,
        season_type_nullable=SEASON_TYPE,
    )
    df = endpoint.get_data_frames()[0]
    save_raw(df, "player_gamelogs")
    return df


def extract_league_leaders() -> pd.DataFrame:
    log.info("► Líderes de estatísticas...")
    categories = ["PTS", "AST", "REB", "STL", "BLK"]
    frames = []
    for stat in categories:
        log.info(f"  → Categoria: {stat}")
        endpoint = safe_request(
            leagueleaders.LeagueLeaders,
            stat_category_abbreviation=stat,
            season=SEASON,
            season_type_all_star=SEASON_TYPE,
        )
        df = endpoint.get_data_frames()[0]
        df["STAT_CATEGORY"] = stat
        frames.append(df)
    result = pd.concat(frames, ignore_index=True)
    save_raw(result, "league_leaders")
    return result


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 55)
    log.info(f"  NBA Pipeline — Temporada: {SEASON}")
    log.info(f"  Saída: {RAW_DIR}")
    log.info("=" * 55)

    steps = [
        ("players_meta", extract_players_meta),
        ("teams_meta", extract_teams_meta),
        ("games", extract_games),
        ("player_gamelogs", extract_player_gamelogs),
        ("league_leaders", extract_league_leaders),
    ]

    results = {}
    for name, func in steps:
        try:
            results[name] = func()
        except Exception as e:
            log.error(f"  ✘ Erro em '{name}': {e}")

    log.info("=" * 55)
    log.info(f"  Concluído. {len(results)}/{len(steps)} etapas com sucesso.")
    log.info("=" * 55)
    return results


if __name__ == "__main__":
    run()
