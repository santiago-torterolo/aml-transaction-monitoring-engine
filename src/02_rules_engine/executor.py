


"""
AML Rules Engine Orchestrator
Executes all detection rules sequentially and consolidates alerts into DuckDB.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rules import detect_structuring
from velocity_rule import detect_velocity_abuse
from round_amounts import detect_round_amounts
from beneficiary_pattern import detect_beneficiary_rotation

def run_all_rules():
    """
    Execute all AML detection rules in sequence.
    Each rule writes to the centralized 'alerts' table.
    """
    print("=" * 60)
    print("AML TRANSACTION MONITORING ENGINE - BATCH RUN")
    print("=" * 60)
    
    rules = [
        ("Structuring Detection", detect_structuring),
        ("Velocity Abuse", detect_velocity_abuse),
        ("Round Amount Pattern", detect_round_amounts),
        ("Beneficiary Rotation", detect_beneficiary_rotation)
    ]
    
    for rule_name, rule_func in rules:
        print(f"\n[RULE] {rule_name}")
        print("-" * 60)
        try:
            rule_func()
        except Exception as e:
            print(f"[CRITICAL ERROR] Rule '{rule_name}' failed: {e}")
        print("-" * 60)
    
    print("\n" + "=" * 60)
    print("[COMPLETED] All rules executed.")
    print("=" * 60)

if __name__ == "__main__":
    run_all_rules()
