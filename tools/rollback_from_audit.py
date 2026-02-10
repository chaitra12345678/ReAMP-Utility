"""
Rollback ServiceNow incident category based on an audit CSV produced by the scheduler.
Usage:
  python tools/rollback_from_audit.py --audit outputs/audit_20260113T115055Z.csv --commit
By default runs in dry-run mode and only prints patch actions.
"""
import argparse
import csv
import os
import sys
# Ensure project root is on sys.path so tools can import top-level modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from connectors.servicenow_connector import ServiceNowConnector


def main(audit_file: str, commit: bool = False):
    if not os.path.exists(audit_file):
        print(f"Audit file not found: {audit_file}")
        return

    connector = ServiceNowConnector(dry_run=not commit)

    actions = []
    with open(audit_file, 'r', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ticket_id = row.get('ticket_id')
            ticket_number = row.get('ticket_number')
            old_category = row.get('old_category') or ''
            new_category = row.get('new_category') or ''
            actions.append((ticket_id, ticket_number, old_category, new_category))

    print(f"Found {len(actions)} entries in audit file. Commit={commit}")

    for ticket_id, ticket_number, old_category, new_category in actions:
        patch_data = {
            'category': old_category,
            'u_auto_classified': 'false',
            'u_auto_classification_category': '',
            'work_notes': f"Rollback: restored category to '{old_category}' (was: '{new_category}')"
        }
        if not commit:
            print(f"[DRY RUN] Would PATCH {ticket_number} ({ticket_id}) -> set category='{old_category}'")
            continue
        # Perform actual PATCH
        success = connector.update_incident(
            ticket_id=ticket_id,
            category=old_category or '',
            confidence=0.0,
            work_notes=patch_data['work_notes'],
            snow_category=old_category or ''
        )
        if success:
            print(f"✓ Patched {ticket_number} ({ticket_id})")
        else:
            print(f"✗ Failed to patch {ticket_number} ({ticket_id})")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rollback ServiceNow incidents from audit CSV')
    parser.add_argument('--audit', required=True, help='Path to audit CSV file')
    parser.add_argument('--commit', action='store_true', help='Apply changes (default is dry-run)')
    args = parser.parse_args()
    main(audit_file=args.audit, commit=args.commit)
