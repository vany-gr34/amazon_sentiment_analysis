"""
training-pipeline/src/pipeline.py
Run the full VADER + RoBERTa pipeline locally without Airflow.

Usage:
    python src/pipeline.py
    python src/pipeline.py --config configs/training.yaml
"""

import argparse
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(config_path: str = None):
    import os
    if config_path:
        os.environ["CONFIG_PATH"] = config_path

    from ingest     import ingest_data
    from preprocess import preprocess_data
    from train      import train_model
    from evaluate   import evaluate_model
    from register   import register_model

    t_total = time.time()
    logger.info("=" * 60)
    logger.info("PIPELINE START  (VADER + RoBERTa)")
    logger.info("=" * 60)

    steps = [
        ("ingest",     ingest_data),
        ("preprocess", preprocess_data),
        ("train",      train_model),       # runs VADER + RoBERTa
        ("evaluate",   evaluate_model),
        ("register",   register_model),
    ]

    results = {}
    for name, fn in steps:
        logger.info(f"\n{'─'*40}\nSTEP: {name.upper()}\n{'─'*40}")
        t0 = time.time()
        try:
            results[name] = fn()
            logger.info(f"[{name.upper()}] Done in {time.time() - t0:.1f}s")
        except Exception as e:
            logger.error(f"[{name.upper()}] FAILED: {e}")
            raise

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {time.time() - t_total:.1f}s")
    logger.info(f"  registered : {results.get('register', {}).get('registered')}")
    logger.info(f"  accuracy   : {results.get('register', {}).get('accuracy')}")
    logger.info("=" * 60)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    run_pipeline(config_path=args.config)