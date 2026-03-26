# Scheduler
# instalar dependências: 
# pip install apscheduler nba_api pandas numpy scipy sqlalchemy psycopg2-binary python-dotenv

import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            Path(__file__).resolve().parent / "pipeline.log", encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def run_pipeline():
    start = datetime.now()
    log.info("=" * 50)
    log.info(f"pipeline iniciado — {start:%Y-%m-%d %H:%M:%S}")

    steps = [
        ("extract_data", "extraindo dados"),
        ("clean_data", "limpando dados"),
        ("transform_data", "calculando métricas"),
        ("load_database", "carregando no banco"),
    ]

    for i, (module_name, label) in enumerate(steps, 1):
        log.info(f"[{i}/{len(steps)}] {label}...")
        try:
            module = __import__(module_name)
            module.run()
        except ImportError as e:
            log.error(f"módulo '{module_name}' não encontrado: {e}")
            return
        except Exception as e:
            log.error(f"falha em '{module_name}': {e}", exc_info=True)
            return

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"pipeline finalizado em {elapsed:.1f}s")


def start_scheduler(hour: int, minute: int):
    import os

    tz = os.getenv("PIPELINE_TZ", "America/New_York")

    scheduler = BlockingScheduler(timezone=tz)
    scheduler.add_job(
        run_pipeline,
        trigger=CronTrigger(hour=hour, minute=minute),
        id="nba_pipeline",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    log.info(f"agendado para {hour:02d}:{minute:02d} ({tz}) — Ctrl+C para parar")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("scheduler encerrado")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NBA pipeline scheduler")
    parser.add_argument("--run-now", action="store_true", help="roda o pipeline agora")
    parser.add_argument("--hour", type=int, default=6, help="hora de execução (0–23)")
    parser.add_argument(
        "--minute", type=int, default=0, help="minuto de execução (0–59)"
    )
    args = parser.parse_args()

    if args.run_now:
        run_pipeline()
    else:
        start_scheduler(args.hour, args.minute)
