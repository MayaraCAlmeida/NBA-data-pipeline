# import dos dados limpo para o PostgreSQL.

import os
import ssl
import logging
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "dados_processados")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# Conexão 

def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:"
        f"{os.getenv('DB_PASSWORD', 'postgres')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{os.getenv('DB_NAME', 'nba_pipeline')}"
    )
    log.info(
        f"  Conectando: {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url, pool_pre_ping=True)


# Helpers 


def upsert(engine, df: pd.DataFrame, table_name: str, pk: list, chunk_size: int = 100):
    """
    Insere ou atualiza registros na tabela. Filtra automaticamente
    as colunas que não existem no banco para evitar erros de schema.
    """
    insp = inspect(engine)
    db_cols = {c["name"] for c in insp.get_columns(table_name)}
    df = df[[c for c in df.columns if c in db_cols]].copy()
    df = df.where(pd.notnull(df), None)
    df = df.drop_duplicates(subset=pk, keep="last")

    meta = MetaData()
    table = Table(table_name, meta, autoload_with=engine)

    with engine.connect() as conn:
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size].to_dict(orient="records")
            stmt = insert(table).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=pk,
                set_={c.key: c for c in stmt.excluded if c.key not in pk},
            )
            conn.execute(stmt)
        conn.commit()

    log.info(f"  ✔ {table_name} ({len(df)} linhas)")


def load_csv(name: str) -> pd.DataFrame | None:
    path = os.path.join(PROCESSED_DIR, f"{name}.csv")
    if not os.path.exists(path):
        log.warning(f"  ✘ {name}.csv não encontrado — pulando")
        return None
    # FIX 1: index_col=False garanti para que nenhuma coluna vire índice,
    # o que causava o erro "Index(['season_year'], dtype='object')" no league_leaders.
    return pd.read_csv(path, index_col=False)


def filter_fk(engine, df: pd.DataFrame, fk_col: str, ref_table: str) -> pd.DataFrame:
    """
    Remove linhas cujo fk_col não existe em ref_table.
    Evita ForeignKeyViolation ao carregar tabelas de fato.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT {fk_col} FROM {ref_table}"))
        valid_ids = {row[0] for row in result}

    before = len(df)
    df = df[df[fk_col].isin(valid_ids)].copy()
    dropped = before - len(df)

    if dropped:
        log.warning(
            f"  filter_fk: {dropped} linha(s) ignorada(s) — "
            f"{fk_col} ausente em '{ref_table}'"
        )

    return df


# Tabelas e ordem de carga 
# A ordem importa: dimensões primeiro, depois fatos.
# FKs em games e player_gamelogs apontam para teams e players.

LOAD_ORDER = [
    # arquivo csv             tabela                   pk                                          chunk
    ("teams_meta", "teams", ["team_id"], 100),
    ("players_meta", "players", ["player_id"], 100),
    ("games", "games", ["team_id", "game_id"], 100),
    (
        "league_leaders",
        "league_leaders",
        ["player_id", "season_year", "stat_category"],
        100,
    ),
    ("player_gamelogs", "player_gamelogs", ["player_id", "game_id"], 50),
    ("player_season_stats", "player_season_stats", ["player_id", "season_year"], 100),
]

# FIX 2: tabelas de fato que precisam de validação de FK antes do upsert.
# Formato: { nome_csv: (coluna_fk, tabela_referenciada) }
FK_CHECKS = {
    "player_gamelogs": ("player_id", "players"),
    "league_leaders": ("player_id", "players"),
    "player_season_stats": ("player_id", "players"),
}


# Main


def run():
    log.info(f"Entrada: {PROCESSED_DIR}")

    try:
        engine = get_engine()
    except Exception as e:
        log.error(f"  ✘ Falha na conexão: {e}")
        raise

    ok = 0
    for filename, table, pk, chunk in LOAD_ORDER:
        df = load_csv(filename)
        if df is None:
            continue

        # FIX 3: league_leaders.csv não tem coluna season_year,
        # mas o banco exige (NOT NULL, parte da PK). Injeta o valor fixo.
        if filename == "league_leaders" and "season_year" not in df.columns:
            df["season_year"] = "2024-25"
            log.info("  league_leaders: season_year='2024-25' adicionado")

        # Aplica filtro de FK se necessário
        if filename in FK_CHECKS:
            fk_col, ref_table = FK_CHECKS[filename]
            df = filter_fk(engine, df, fk_col, ref_table)

        try:
            upsert(engine, df, table, pk, chunk_size=chunk)
            ok += 1
        except Exception as e:
            log.error(f"  ✘ {table}: {e}")

    log.info(f"Concluído — {ok}/{len(LOAD_ORDER)} tabelas carregadas.")


if __name__ == "__main__":
    run()
