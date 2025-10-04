#!/usr/bin/env python3
"""Enhanced client for the Project Sentinel event stream with structured JSON alerts."""

import argparse
import json
import socket
import uuid
from datetime import datetime
from typing import Iterator

# --- thresholds ---
MAX_QUEUE = 6
MAX_WAIT = 300  # seconds

# --- state memory ---
inventory_snapshots = []
pos_data = []
rfid_data = []
recog_data = []
queue_data = []

# simple counter for event IDs
event_counter = 0


# -------------------------------
# Event Reader
# -------------------------------
def read_events(host: str, port: int) -> Iterator[dict]:
    with socket.create_connection((host, port)) as conn:
        with conn.makefile("r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                yield json.loads(line)


# -------------------------------
# JSON Event Builder
# -------------------------------
def build_event(event_name: str, data: dict) -> str:
    global event_counter
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    event_id = f"E{event_counter:03d}"
    event_counter += 1

    return json.dumps({
        "timestamp": ts,
        "event_id": event_id,
        "event_data": {
            "event_name": event_name,
            **data
        }
    })


# -------------------------------
# Rule Processing
# -------------------------------
def process_event(event: dict):
    dataset = event.get("dataset")
    payload = event.get("event")  # replay server wraps the payload inside "event"

    outputs = []

    # Inventory discrepancy
    if dataset == "Current_inventory_data":
        inventory_snapshots.append(payload)
        if len(inventory_snapshots) >= 2:
            before = inventory_snapshots[-2]["data"]
            after = inventory_snapshots[-1]["data"]
            for sku in before:
                expected = before[sku]
                actual = after.get(sku, 0)
                if expected != actual:
                    outputs.append(build_event("Inventory Discrepancy", {
                        "SKU": sku,
                        "Expected_Inventory": expected,
                        "Actual_Inventory": actual
                    }))

    # Queue monitoring
    elif dataset == "Queue_monitor":
        queue_data.append(payload)
        c = payload["data"]["customer_count"]
        w = payload["data"]["average_dwell_time"]
        station = payload["station_id"]
        if c > MAX_QUEUE:
            outputs.append(build_event("Long Queue Length", {
                "station_id": station,
                "num_of_customers": c
            }))
        if w > MAX_WAIT:
            outputs.append(build_event("Long Wait Time", {
                "station_id": station,
                "wait_time_seconds": w
            }))

    # RFID
    elif dataset == "RFID_data":
        rfid_data.append(payload)
        sku = payload["data"].get("sku")
        if payload["status"] == "Read Error":
            outputs.append(build_event("RFID Read Error", {
                "station_id": payload["station_id"]
            }))
        if sku and sku != "null":
            if not any(p['data']['sku'] == sku for p in pos_data):
                outputs.append(build_event("Scanner Avoidance", {
                    "station_id": payload["station_id"],
                    "customer_id": payload.get("customer_id", "Unknown"),
                    "product_sku": sku
                }))

        # POS
    elif dataset == "POS_data":
        pos_data.append(payload)

        if payload["status"] == "System Crash":
            outputs.append(build_event("Unexpected Systems Crash", {
                "station_id": payload["station_id"],
                "duration_seconds": 180  # placeholder, could calculate from timestamps
            }))
        elif payload["status"] == "Active":
            outputs.append(build_event("Succes Operation", {
                "station_id": payload["station_id"],
                "customer_id": payload["data"].get("customer_id", "Unknown"),
                "product_sku": payload["data"].get("sku", "Unknown")
            }))


    # Product recognition
    elif dataset == "Product_recognition":
        recog_data.append(payload)
        sku_pred = payload["data"].get("predicted_product")
        for p in pos_data:
            if p["station_id"] == payload["station_id"]:
                if p["data"]["sku"] != sku_pred:
                    outputs.append(build_event("Barcode Switching", {
                        "station_id": payload["station_id"],
                        "customer_id": p["data"]["customer_id"],
                        "actual_sku": sku_pred,
                        "scanned_sku": p["data"]["sku"]
                    }))

    return outputs


# -------------------------------
# Main
# -------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Consume events from the replay server with detection rules")
    parser.add_argument("--host", default="172.17.6.242")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--limit", type=int, default=0, help="Stop after N events (0 = unlimited)")
    args = parser.parse_args()

    for idx, event in enumerate(read_events(args.host, args.port), start=1):
        for out in process_event(event):
            print(out)
        if args.limit and idx >= args.limit:
            break


if __name__ == "__main__":
    main()
