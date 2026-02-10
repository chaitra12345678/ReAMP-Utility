"""
Run Task Classifier against real ServiceNow incidents (single cycle)

Usage:
  python run_task_classifier_snow.py        # dry-run (no updates)
  python run_task_classifier_snow.py --commit  # apply updates to ServiceNow
"""

import argparse
import os
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scheduler import TaskParserScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Run Task Classifier against ServiceNow incidents (single cycle)')
    parser.add_argument('--commit', action='store_true', help='Apply updates to ServiceNow (default is dry-run)')
    parser.add_argument('--limit', type=int, default=50, help='Max number of incidents to retrieve')
    parser.add_argument('--sample-size', type=int, default=0, help='If >0, process only the first N incidents (staged test)')
    args = parser.parse_args()

    # Dry-run if not commit
    dry_run = not args.commit

    logger.info(f"Starting ServiceNow run (dry_run={dry_run})")

    # Initialize scheduler with live ServiceNow connector
    scheduler = TaskParserScheduler(use_mock=False, dry_run=dry_run)

    # Run a single processing cycle
    sample_size = args.sample_size if args.sample_size > 0 else None
    stats = scheduler.process_tickets(limit=args.limit, sample_size=sample_size)

    logger.info("Run complete.")
    logger.info(f"Cycle stats: {stats}")


if __name__ == '__main__':
    main()
