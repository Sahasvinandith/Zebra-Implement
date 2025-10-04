import csv
import json
import os

# --- FILE PATHS ---
INPUT_DIR = "input"
CSV_FILE = os.path.join(INPUT_DIR, "products_list.csv")
JSONL_FILE = os.path.join(INPUT_DIR, "inventory_snapshots.jsonl")

# --- STEP 1: Load initial quantities from CSV ---
initial_quantities = {}
with open(CSV_FILE, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        initial_quantities[row["SKU"]] = int(row["quantity"])

# --- STEP 2: Compare with each snapshot ---
differences_per_snapshot = []

with open(JSONL_FILE, "r", encoding="utf-8") as f:
    for line in f:
        snapshot = json.loads(line)
        timestamp = snapshot.get("timestamp", "unknown_snapshot")
        data = snapshot.get("data", {})

        snapshot_diffs = []

        for sku, current_qty in data.items():
            if sku in initial_quantities:
                initial_qty = initial_quantities[sku]
                diff = current_qty - initial_qty

                if diff != 0:
                    snapshot_diffs.append({
                        "sku": sku,
                        "initial_quantity": initial_qty,
                        "current_quantity": current_qty,
                        "difference": diff
                    })

        # Add only if there are differences
        if snapshot_diffs:
            differences_per_snapshot.append({
                "timestamp": timestamp,
                "differences": snapshot_diffs
            })

# --- STEP 3: Print result ---
print(json.dumps(differences_per_snapshot, indent=2))
