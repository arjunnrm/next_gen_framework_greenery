"""
Generate sample data for soft-delete feature testing.

This script creates:
1. Sample Bronze table data (customers)
2. Sample key file (customer deletion marks)
3. Instructions for testing

Usage:
    python soft_delete_sample_data.py --output-dir /path/to/test/data
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any


def create_bronze_sample_data() -> List[Dict[str, Any]]:
    """Create sample Bronze customer data."""
    return [
        {
            "customer_id": "CUST001",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "phone": "555-0001",
            "status": "active",
            "created_date": "2024-01-15",
        },
        {
            "customer_id": "CUST002",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "phone": "555-0002",
            "status": "active",
            "created_date": "2024-01-16",
        },
        {
            "customer_id": "CUST003",
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "phone": "555-0003",
            "status": "inactive",
            "created_date": "2024-01-17",
        },
        {
            "customer_id": "CUST004",
            "name": "Diana Prince",
            "email": "diana@example.com",
            "phone": "555-0004",
            "status": "active",
            "created_date": "2024-01-18",
        },
        {
            "customer_id": "CUST005",
            "name": "Eve Wilson",
            "email": "eve@example.com",
            "phone": "555-0005",
            "status": "active",
            "created_date": "2024-01-19",
        },
        {
            "customer_id": "CUST006",
            "name": "Frank Miller",
            "email": "frank@example.com",
            "phone": "555-0006",
            "status": "active",
            "created_date": "2024-01-20",
        },
        {
            "customer_id": "CUST007",
            "name": "Grace Lee",
            "email": "grace@example.com",
            "phone": "555-0007",
            "status": "suspended",
            "created_date": "2024-01-21",
        },
        {
            "customer_id": "CUST008",
            "name": "Henry Zhang",
            "email": "henry@example.com",
            "phone": "555-0008",
            "status": "active",
            "created_date": "2024-01-22",
        },
        {
            "customer_id": "CUST009",
            "name": "Iris Kim",
            "email": "iris@example.com",
            "phone": "555-0009",
            "status": "active",
            "created_date": "2024-01-23",
        },
        {
            "customer_id": "CUST010",
            "name": "Jack Davis",
            "email": "jack@example.com",
            "phone": "555-0010",
            "status": "active",
            "created_date": "2024-01-24",
        },
    ]


def create_key_file_data() -> List[Dict[str, Any]]:
    """Create sample key file with deletion marks.

    Marks these customers for deletion:
    - CUST003 (Charlie Brown - inactive)
    - CUST007 (Grace Lee - suspended)

    These remain active:
    - CUST001, CUST002, CUST004, CUST005, CUST006, CUST008, CUST009, CUST010
    """
    return [
        {"customer_id": "CUST001", "is_marked_deleted": False},
        {"customer_id": "CUST002", "is_marked_deleted": False},
        {"customer_id": "CUST003", "is_marked_deleted": True},  # MARKED FOR DELETION
        {"customer_id": "CUST004", "is_marked_deleted": False},
        {"customer_id": "CUST005", "is_marked_deleted": False},
        {"customer_id": "CUST006", "is_marked_deleted": False},
        {"customer_id": "CUST007", "is_marked_deleted": True},  # MARKED FOR DELETION
        {"customer_id": "CUST008", "is_marked_deleted": False},
        {"customer_id": "CUST009", "is_marked_deleted": False},
        {"customer_id": "CUST010", "is_marked_deleted": False},
    ]


def create_incremental_key_file_data_run2() -> List[Dict[str, Any]]:
    """Create updated key file for second run with additional deletions.

    Now marks these for deletion:
    - CUST003 (already deleted in run 1)
    - CUST007 (already deleted in run 1)
    - CUST005 (NEW - newly marked for deletion)
    """
    return [
        {"customer_id": "CUST001", "is_marked_deleted": False},
        {"customer_id": "CUST002", "is_marked_deleted": False},
        {"customer_id": "CUST003", "is_marked_deleted": True},  # Already deleted
        {"customer_id": "CUST004", "is_marked_deleted": False},
        {"customer_id": "CUST005", "is_marked_deleted": True},  # NEW
        {"customer_id": "CUST006", "is_marked_deleted": False},
        {"customer_id": "CUST007", "is_marked_deleted": True},  # Already deleted
        {"customer_id": "CUST008", "is_marked_deleted": False},
        {"customer_id": "CUST009", "is_marked_deleted": False},
        {"customer_id": "CUST010", "is_marked_deleted": False},
    ]


def save_as_json(data: List[Dict[str, Any]], filepath: Path) -> None:
    """Save data as JSON file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Created: {filepath}")


def generate_sample_data(output_dir: str = None) -> None:
    """Generate all sample data files."""
    if output_dir is None:
        output_dir = str(Path(__file__).parent)

    output_path = Path(output_dir)

    print("\n" + "="*70)
    print("SOFT-DELETE FEATURE: SAMPLE DATA GENERATION")
    print("="*70 + "\n")

    # Bronze table data
    bronze_data = create_bronze_sample_data()
    print("[BRONZE TABLE DATA] Customers")
    print("-" * 70)
    print("Records: {}".format(len(bronze_data)))
    for record in bronze_data:
        print("  {} | {} | {}".format(
            record['customer_id'],
            record['name'].ljust(20),
            record['status']
        ))

    save_as_json(bronze_data, output_path / "bronze_customers.json")

    # Key file - Run 1
    key_data_run1 = create_key_file_data()
    print("\n[KEY FILE DATA] RUN 1 - Initial Deletions")
    print("-" * 70)
    marked_for_deletion = [d for d in key_data_run1 if d["is_marked_deleted"]]
    print("Total keys: {}".format(len(key_data_run1)))
    print("Marked for deletion: {}".format(len(marked_for_deletion)))
    for record in marked_for_deletion:
        print("  {} -> MARKED FOR DELETION".format(record['customer_id']))

    save_as_json(key_data_run1, output_path / "customer_deletion_keys_run1.json")

    # Key file - Run 2 (incremental)
    key_data_run2 = create_incremental_key_file_data_run2()
    print("\n[KEY FILE DATA] RUN 2 - Incremental Deletions")
    print("-" * 70)
    marked_for_deletion_run2 = [d for d in key_data_run2 if d["is_marked_deleted"]]
    new_deletes = [
        d["customer_id"] for d in marked_for_deletion_run2
        if d not in marked_for_deletion
    ]
    print("Total keys: {}".format(len(key_data_run2)))
    print("Marked for deletion: {}".format(len(marked_for_deletion_run2)))
    print("New deletions (from run 1): {}".format(new_deletes if new_deletes else 'None'))
    for record in marked_for_deletion_run2:
        marker = " (NEW)" if record["customer_id"] in new_deletes else ""
        print("  {} -> MARKED FOR DELETION{}".format(record['customer_id'], marker))

    save_as_json(key_data_run2, output_path / "customer_deletion_keys_run2.json")

    # Expected results
    print("\n[EXPECTED TEST RESULTS] Summary")
    print("-" * 70)
    print("\nRUN 1 (Initial soft-delete):")
    print("  Input Bronze table:  10 records")
    print("  Key file deletions:  2 records (CUST003, CUST007)")
    print("  Result:")
    print("    - Soft-deleted rows: 2")
    print("    - Active rows:       8")
    print("    - is_deleted = true: CUST003, CUST007")

    print("\nRUN 2 (Incremental soft-delete with updated key file):")
    print("  Input Bronze table:  10 records")
    print("  Key file deletions:  3 records (CUST003, CUST005, CUST007)")
    print("  Result:")
    print("    - Soft-deleted rows: 3")
    print("    - Active rows:       7")
    print("    - is_deleted = true: CUST003, CUST005, CUST007")
    print("    - Note: Run 1 deletes unchanged; new delete added (CUST005)")

    print("\nRUN 3 (Idempotency test - same key file as Run 2):")
    print("  Input Bronze table:  10 records (same as Run 2 result)")
    print("  Key file deletions:  3 records (identical to Run 2)")
    print("  Result:")
    print("    - Soft-deleted rows: 3 (NO CHANGE)")
    print("    - Active rows:       7 (NO CHANGE)")
    print("    - [OK] Idempotent: identical result")

    print("\n" + "="*70)
    print("DATA FILES GENERATED:")
    print("="*70)
    print("See .json files in: {}".format(output_path))
    print("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate sample data for soft-delete feature testing"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for sample data files (default: current directory)"
    )
    args = parser.parse_args()

    generate_sample_data(args.output_dir)
