"""
Task Parser Scheduler
Automatically retrieves, classifies, and updates incidents
"""
import schedule
import os
import sys
import json
import logging
import time
from datetime import datetime
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.ticket_classifier import TaskClassifier
from connectors.servicenow_connector import ServiceNowConnector, MockServiceNowConnector
from utils.logger import setup_logger

# Initialize logging
logger = setup_logger(__name__, log_level=os.getenv('LOG_LEVEL', 'INFO'))

class TaskParserScheduler:
    """
    Scheduler for automated incident classification and routing
    """
    
    def __init__(self, use_mock: bool = False, dry_run: bool | None = None):
        """
        Initialize scheduler
        
        Args:
            use_mock: If True, use mock ServiceNow connector for testing
            dry_run: If not None, explicitly set dry-run mode (overrides DRY_RUN env var)
        """
        self.parser = TaskClassifier()

        # Determine dry-run mode: explicit argument overrides env var
        if dry_run is None:
            self.dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'
        else:
            self.dry_run = bool(dry_run)
        
        if use_mock:
            logger.info("🧪 Using MOCK ServiceNow connector for testing")
            self.connector = MockServiceNowConnector()
        else:
            self.connector = ServiceNowConnector(dry_run=self.dry_run)
            if not self.connector._check_connection():
                logger.warning("⚠ ServiceNow connection failed - will retry on schedule")
        
        self.stats = {
            'total_processed': 0,
            'successfully_classified': 0,
            'successfully_updated': 0,
            'errors': 0,
            'start_time': datetime.utcnow()
        }
        
        logger.info(f"✓ TaskParserScheduler initialized")
        logger.info(f"  - Dry Run: {self.dry_run}")
        logger.info(f"  - Schedule Interval: {self.get_schedule_interval()} seconds")
    
    @staticmethod
    def get_schedule_interval() -> int:
        """Get schedule interval from environment"""
        return int(os.getenv('SCHEDULE_INTERVAL', 300))
    
    def process_tickets(self, limit: int = 50, sample_size: int | None = None) -> Dict:
        """
        Main processing function - retrieve, classify, and update incidents
        
        Args:
            limit: max number of incidents to fetch from ServiceNow
            sample_size: optional number of incidents to process (staged test)
        Returns:
            Statistics dictionary
        """
        logger.info("=" * 60)
        logger.info(f"🔄 Starting ticket processing cycle at {datetime.utcnow().isoformat()}")
        logger.info("=" * 60)
        
        cycle_stats = {
            'retrieved': 0,
            'classified': 0,
            'updated': 0,
            'errors': 0
        }
        
        try:
            # Step 1: Retrieve incidents
            incidents = self.connector.get_new_incidents(limit=limit)
            cycle_stats['retrieved'] = len(incidents)
            
            if not incidents:
                logger.info("ℹ No new incidents to process")
                return cycle_stats
            
            # Apply sampling for staged tests (if requested)
            if sample_size and sample_size > 0:
                logger.info(f"⚠ Running staged test: processing only first {sample_size} of {len(incidents)} incidents")
                incidents = incidents[:sample_size]

            logger.info(f"\n📋 Processing {len(incidents)} incidents...")
            
            # Load choices once from ServiceNow
            category_choices = self.connector.get_choice_values("category")
            subcategory_choices = self.connector.get_choice_values("subcategory")
            priority_values = self.connector.get_priority_lookup_values()
            impact_choices = priority_values.get("impact", [])
            urgency_choices = priority_values.get("urgency", [])

            if not category_choices or not impact_choices or not urgency_choices:
                logger.error("✗ Missing category/impact/urgency choices; aborting cycle")
                return cycle_stats

            # Prepare audit CSV if not a dry-run
            audit_csv_path = None
            run_ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
            if not self.dry_run:
                audit_dir = os.path.join(os.path.dirname(__file__), 'outputs')
                os.makedirs(audit_dir, exist_ok=True)
                audit_csv_path = os.path.join(audit_dir, f'audit_{run_ts}.csv')

            # Step 2: Classify each incident
            for idx, incident in enumerate(incidents, 1):
                try:
                    ticket_id = incident.get('sys_id')
                    ticket_number = incident.get('number')
                    description = incident.get('short_description', '')
                    old_category = incident.get('category')
                    
                    logger.info(f"\n[{idx}/{len(incidents)}] Processing {ticket_number}")
                    logger.debug(f"  Description: {description[:80]}...")
                    
                    # Classify via Azure OpenAI
                    start_time = time.perf_counter()
                    result = self.parser.classify(
                        description=description,
                        category_choices=category_choices,
                        subcategory_choices=subcategory_choices,
                        impact_choices=impact_choices,
                        urgency_choices=urgency_choices
                    )
                    end_time = time.perf_counter()
                    exec_ms = int((end_time - start_time) * 1000)

                    if result["status"] != "SUCCESS":
                        logger.warning(f"  ⚠ Classification failed: {result.get('summary')}")
                        cycle_stats["errors"] += 1
                        continue

                    details = result["details"]
                    category = details["category"]
                    subcategory = details.get("subcategory")
                    impact = details["impact"]
                    urgency = details["urgency"]
                    confidence = details.get("confidence", 0.0)
                    matched_keywords = []

                    logger.info(f"  ✓ Category: {category} | Subcategory: {subcategory} | Impact: {impact} | Urgency: {urgency}")
                    cycle_stats['classified'] += 1

                    # Persist classification result to outputs (append-only JSON + CSV)
                    try:
                        from tools.output_writer import append_task_result
                        append_task_result(
                            ticket_id=ticket_id,
                            input_text=description,
                            category=category,
                            confidence=float(confidence),
                            matched_keywords=matched_keywords,
                            execution_time_ms=exec_ms
                        )
                    except Exception:
                        logger.warning('⚠ Failed to persist classification result to outputs')
                    
                    work_notes = (
                        f"Auto-classified as {category} ({confidence:.1%})\n"
                        f"Keywords matched: {', '.join(matched_keywords)}\n"
                        f"Processed by Task Classifier at {datetime.utcnow().isoformat()}"
                    )
                    
                    # Step 3: Update ServiceNow
                    success = self.connector.update_incident(
                        ticket_id=ticket_id,
                        category=category,
                        confidence=confidence,
                        work_notes=work_notes,
                        snow_category=category,
                        subcategory=subcategory,
                        impact=impact,
                        urgency=urgency
                    )
                    
                    if success:
                        logger.info("  ✅ Updated: Category/Subcategory/Impact/Urgency")
                        cycle_stats['updated'] += 1

                        # Write audit entry when not dry-run
                        try:
                            if audit_csv_path:
                                from tools.output_writer import append_audit_entry
                                append_audit_entry(
                                    audit_csv_path=audit_csv_path,
                                    ticket_number=ticket_number,
                                    ticket_id=ticket_id,
                                    old_category=old_category,
                                    new_category=category,
                                    confidence=confidence,
                                    run_timestamp=run_ts,
                                    dry_run=self.dry_run,
                                )
                        except Exception:
                            logger.warning('⚠ Failed to write audit entry')

                    else:
                        logger.error(f"  ❌ Failed to update incident")
                        cycle_stats['errors'] += 1
                    
                except Exception as e:
                    logger.error(f"  ❌ Error processing incident: {str(e)}")
                    cycle_stats['errors'] += 1
                    continue
            
            # Update overall stats
            self.stats['total_processed'] += cycle_stats['classified']
            self.stats['successfully_updated'] += cycle_stats['updated']
            self.stats['errors'] += cycle_stats['errors']
            
        except Exception as e:
            logger.error(f"❌ Fatal error in processing cycle: {str(e)}")
            self.stats['errors'] += 1
        
        # Log summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 Cycle Summary:")
        logger.info(f"  Retrieved: {cycle_stats['retrieved']}")
        logger.info(f"  Classified: {cycle_stats['classified']}")
        logger.info(f"  Updated: {cycle_stats['updated']}")
        logger.info(f"  Errors: {cycle_stats['errors']}")
        logger.info("=" * 60 + "\n")
        
        return cycle_stats
    
    def start(self):
        """Start the scheduler"""
        interval = self.get_schedule_interval()
        
        logger.info("=" * 60)
        logger.info("🚀 TASK PARSER SCHEDULER STARTED")
        logger.info("=" * 60)
        logger.info(f"  Schedule Interval: {interval} seconds ({interval // 60} minutes)")
        logger.info(f"  Dry Run Mode: {self.dry_run}")
        logger.info(f"  Log Level: {os.getenv('LOG_LEVEL', 'INFO')}")
        logger.info("=" * 60 + "\n")
        
        # Schedule the job
        schedule.every(interval).seconds.do(self.process_tickets)
        
        # Run initial cycle immediately
        logger.info("ℹ Running initial processing cycle...")
        self.process_tickets()
        
        # Start scheduler loop
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n\n🛑 Scheduler stopped by user")
            self.print_final_stats()
    
    def print_final_stats(self):
        """Print final statistics"""
        uptime = datetime.utcnow() - self.stats['start_time']
        logger.info("\n" + "=" * 60)
        logger.info("📈 FINAL STATISTICS")
        logger.info("=" * 60)
        logger.info(f"  Uptime: {uptime}")
        logger.info(f"  Total Processed: {self.stats['total_processed']}")
        logger.info(f"  Successfully Updated: {self.stats['successfully_updated']}")
        logger.info(f"  Errors: {self.stats['errors']}")
        logger.info("=" * 60)


def process_json_file(filepath: str, use_mock: bool = True) -> Dict:
    """
    Test function to process tickets from JSON file
    
    Args:
        filepath: Path to JSON file with test tickets
        use_mock: If True, use mock connector (no actual ServiceNow updates)
        
    Returns:
        Statistics dictionary
    """
    logger.info(f"\n🧪 TEST MODE: Processing tickets from {filepath}")
    
    try:
        with open(filepath, 'r') as f:
            tickets = json.load(f)
        
        logger.info(f"✓ Loaded {len(tickets)} tickets from JSON")
        
        # Convert tickets to ServiceNow format
        test_incidents = []
        for idx, ticket in enumerate(tickets, 1):
            if not ticket.get('description'):
                continue
            
            test_incidents.append({
                'sys_id': f"TEST-{idx:04d}",
                'number': ticket.get('ticket_id', f"TEST-{idx}"),
                'short_description': ticket['description'],
                'description': ticket.get('text', ''),
                'state': 1,
                'priority': 3
            })
        
        # Process with mock connector
        scheduler = TaskParserScheduler(use_mock=True)
        scheduler.connector.test_data = test_incidents
        
        stats = scheduler.process_tickets()
        
        # Show mock updates
        if hasattr(scheduler.connector, 'get_updates'):
            logger.info("\n📝 Mock Updates Summary:")
            for update in scheduler.connector.get_updates()[:10]:  # Show first 10
                logger.info(f"  {update['ticket_id']}: {update['category']} ({update['confidence']:.1%})")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error processing JSON file: {str(e)}")
        return {}


if __name__ == '__main__':
    # Check if we should run in test mode
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        # Test mode: process JSON file
        test_file = sys.argv[2] if len(sys.argv) > 2 else 'test_payloads.json'
        process_json_file(test_file)
    else:
        # Production mode: start scheduler
        scheduler = TaskParserScheduler(use_mock=False)
        scheduler.start()
