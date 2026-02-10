"""
Validate task_classifier/category_mapping.yaml against ServiceNow 'category' choices.
Produces a report of any mapping values not present in ServiceNow choices and suggests close matches.
Optionally, with --apply it can update the mapping file using the closest match for each missing value.

Usage:
  python tools/validate_mapping.py        # write report to outputs/mapping_suggestions.json
  python tools/validate_mapping.py --apply  # attempt to apply best-match suggestions to mapping.yaml

"""
import argparse
import os
import sys
import json
import difflib
# Ensure project root is on sys.path so tools can import top-level modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from connectors.servicenow_connector import ServiceNowConnector

MAPPING_PATH = os.path.join(os.path.dirname(__file__), '..', 'task_classifier', 'category_mapping.yaml')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'outputs', 'mapping_suggestions.json')


def load_mapping():
    import yaml
    if not os.path.exists(MAPPING_PATH):
        return {}
    with open(MAPPING_PATH, 'r', encoding='utf-8') as fh:
        return yaml.safe_load(fh) or {}


def save_mapping(mapping):
    import yaml
    with open(MAPPING_PATH, 'w', encoding='utf-8') as fh:
        yaml.safe_dump(mapping, fh, default_flow_style=False, sort_keys=False)


def main(apply: bool = False):
    connector = ServiceNowConnector()
    choices = connector.get_category_choices()
    if not choices:
        print("No category choices available from ServiceNow; cannot validate mapping.")
        return

    mapping = load_mapping()
    report = {
        'available_choices_count': len(choices),
        'missing_mappings': []
    }

    for internal_cat, mapped_value in mapping.items():
        if not mapped_value:
            report['missing_mappings'].append({'category': internal_cat, 'issue': 'empty', 'suggestion': None})
            continue
        if mapped_value in choices:
            continue
        # Suggest a close match
        suggestion = difflib.get_close_matches(mapped_value, choices, n=1)
        suggestion = suggestion[0] if suggestion else None
        report['missing_mappings'].append({'category': internal_cat, 'mapped_value': mapped_value, 'suggestion': suggestion})

    # Write report
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as fh:
        json.dump(report, fh, indent=2)

    print(f"Wrote mapping validation report to {OUTPUT_PATH}")

    if apply and report['missing_mappings']:
        changed = False
        for entry in report['missing_mappings']:
            if entry.get('suggestion'):
                mapping[entry['category']] = entry['suggestion']
                changed = True
        if changed:
            save_mapping(mapping)
            print(f"Applied suggested fixes to {MAPPING_PATH}")
        else:
            print("No automatic suggestions available to apply.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Validate and suggest fixes for category mapping')
    parser.add_argument('--apply', action='store_true', help='Apply suggested fixes to mapping.yaml')
    args = parser.parse_args()
    main(apply=args.apply)
