"""
NBA Data Pipeline - Automation Scheduler
instalar dependências: pip install apscheduler nba_api pandas numpy scipy sqlalchemy psycopg2-binary python-dotenv
"""

import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Adiciona a pasta atual ao path (todos os modulos estao na mesma pasta)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# --- Logger ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            str(Path(__file__).resolve().parent / "pipeline.log"), encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# --- Pipeline ----------------------------------------------------------------
def run_full_pipeline():
    """Executa o pipeline completo de dados NBA."""
    start = datetime.now()
    log.info("=" * 60)
    log.info(f"NBA Pipeline iniciado em {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        # Step 1 - Extract
        log.info("[1/4] Extraindo dados...")
        from extract_data import run

        run()

        # Step 2 - Clean
        log.info("[2/4] Limpando dados...")
        from clean_data import run

        run()

        # Step 3 - Transform
        log.info("[3/4] Transformando & calculando metricas...")
        from transform_data import run

        run()

        # Step 4 - Load
        log.info("[4/4] Carregando no banco de dados...")
        from load_database import run

        run()

        elapsed = (datetime.now() - start).seconds
        log.info(f"Pipeline concluido com sucesso em {elapsed}s")

    except Exception as e:
        log.error(f"Pipeline FALHOU: {e}", exc_info=True)
        raise


# --- Scheduler ---------------------------------------------------------------
def start_scheduler(hour: int = 6, minute: int = 0):
    """
    Agenda o pipeline para rodar todo dia as {hour}:{minute}.
    Padrao: 06:00 AM -- logo apos os jogos noturnos terminarem.
    """
    scheduler = BlockingScheduler(timezone="America/New_York")

    scheduler.add_job(
        run_full_pipeline,
        trigger=CronTrigger(hour=hour, minute=minute),
        id="nba_daily_pipeline",
        name="NBA Daily Pipeline",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    log.info(
        f"Scheduler iniciado -- pipeline roda diariamente as {hour:02d}:{minute:02d} ET"
    )
    log.info("   Pressione Ctrl+C para parar.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler encerrado.")


# --- Entry Point -------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NBA Data Pipeline Scheduler")
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Executa o pipeline uma vez imediatamente",
    )
    parser.add_argument(
        "--hour", type=int, default=6, help="Hora para rodar diariamente (ET, 0-23)"
    )
    parser.add_argument(
        "--minute", type=int, default=0, help="Minuto para rodar diariamente (0-59)"
    )
    args = parser.parse_args()

    if args.run_now:
        run_full_pipeline()
    else:
        start_scheduler(hour=args.hour, minute=args.minute)
