#!/usr/bin/env python3
"""Enhanced client for the Project Sentinel event stream with structured JSON alerts."""

import argparse
import json
import socket
import uuid
from datetime import datetime
from typing import Iterator

Opened_checkouts_queues = {}

def save_state():
    """Saves the current global state to a JSON file."""
    global Opened_checkouts_queues
    try:
        with open("queue_state.json", "w") as f:
            json.dump(Opened_checkouts_queues, f, indent=4)
    except Exception as e:
        print(f"Error saving state to file: {e}")

def checkout_update(event):
    global Opened_checkouts_queues
    MIN_QUEUE_THRESHOLD = 6
    MAX_AVG_TIME_THRESHOLD = 120  # Example: 120 seconds (2 minutes) average wait time
    MIN_OPEN_RC1 = 1
    MIN_OPEN_SCC = 1 # Corrected to SCC

    # 1. Update the current queue status
    station_id = event["station_id"]
    queue_data = event['data']
    Opened_checkouts_queues[station_id] = queue_data

    # 2. Print the current status (for logging)
    print(f"Current Checkouts Status: {Opened_checkouts_queues}")

    # 3. Create a dictionary of current counts and times for easier logic
    current_metrics = {}
    for sid, data in Opened_checkouts_queues.items():
        try:
            # Extract and convert count and average time from the stored dictionary data
            current_metrics[sid] = {
                "count": int(data.get("customer_count", 0)),
                "avg_time": float(data.get("average_time", 0.0))
            }
        except (TypeError, ValueError):
            print(f"ERROR: Invalid data (count or avg_time) for station {sid}. Skipping analysis.")
            return # Stop analysis if data is bad

    # Count the currently active RC1 and SCC stations
    rc1_stations_open = sum(1 for sid in current_metrics if sid.startswith("RC1"))
    scc_stations_open = sum(1 for sid in current_metrics if sid.startswith("SCC")) # <-- FIXED to "SCC"

    # 4. Check for conditions requiring an OPEN command (Load Balancing)

    # A. Check for long average time (Priority check)
    for sid, metrics in current_metrics.items():
        avg_time = metrics["avg_time"]
        if avg_time > MAX_AVG_TIME_THRESHOLD:
            print(f"COMMAND: Average time at {sid} is {avg_time:.2f}s. This is too long. OPEN another station to improve flow.")
            return

    # B. Check for long queues (Secondary check)
    for sid, metrics in current_metrics.items():
        queue_length = metrics["count"]
        if queue_length > MIN_QUEUE_THRESHOLD:
            print(f"COMMAND: Queue at {sid} has {queue_length} people. OPEN another station to balance load.")
            return

    # 5. Check for conditions requiring a CLOSE command (Efficiency)
    can_close_rc1 = rc1_stations_open > MIN_OPEN_RC1
    can_close_scc = scc_stations_open > MIN_OPEN_SCC # Corrected to SCC

    for sid, metrics in current_metrics.items():
        queue_length = metrics["count"]
        if queue_length == 0:
            # Check if this station is an RC1 and we can close an RC1
            if sid.startswith("RC1") and can_close_rc1:
                # Check if all other *open* stations have manageable queues (<= threshold)
                # Check based on count AND time to be safe for closure
                if all(m['count'] <= MIN_QUEUE_THRESHOLD and m['avg_time'] <= MAX_AVG_TIME_THRESHOLD for osid, m in current_metrics.items() if osid != sid):
                    print(f"COMMAND: Station {sid} has 0 queue. All others manageable. CLOSE {sid}.")
                    return

            # Check if this station is an SCC and we can close an SCC
            elif sid.startswith("SCC") and can_close_scc: # Corrected to SCC
                # Check if all other *open* stations have manageable queues (<= threshold)
                if all(m['count'] <= MIN_QUEUE_THRESHOLD and m['avg_time'] <= MAX_AVG_TIME_THRESHOLD for osid, m in current_metrics.items() if osid != sid):
                    print(f"COMMAND: Station {sid} has 0 queue. All others manageable. CLOSE {sid}.")
                    return

    # 6. Check minimum required stations are up (Startup/Recovery check)
    if rc1_stations_open < MIN_OPEN_RC1:
        print(f"ALERT: Only {rc1_stations_open} RC1 stations are open. Need {MIN_OPEN_RC1}. COMMAND: OPEN an RC1 station.")
        return

    if scc_stations_open < MIN_OPEN_SCC: # Corrected to SCC
        print(f"ALERT: Only {scc_stations_open} SCC stations are open. Need {MIN_OPEN_SCC}. COMMAND: OPEN an SCC station.")
        return

    # If no conditions were met
    print("STATUS: Queues are balanced and minimum stations are open. No action required.")
    save_state()

# def checkout_update(event):
#     global Opened_checkouts_queues
#     MIN_QUEUE_THRESHOLD = 6
#     MIN_OPEN_RC1 = 1
#     MIN_OPEN_SCCX = 1
#
#     # 1. Update the current queue status
#     station_id = event["station_id"]
#     queue_data = event['data']
#     Opened_checkouts_queues[station_id] = queue_data
#
#     # 2. Print the current status (for logging)
#     print(f"Current Checkouts Status: {Opened_checkouts_queues}")
#
#     # 3. Analyze current open stations and queues
#
#     # Count the currently active RC1 and SCCx stations
#     rc1_stations_open = sum(1 for sid in Opened_checkouts_queues if sid.startswith("RC1"))
#     sccx_stations_open = sum(1 for sid in Opened_checkouts_queues if sid.startswith("SCC"))
#
#     # Check for long queues (OPEN a station)
#     for sid, station_data in Opened_checkouts_queues.items():
#         queue_length = int(station_data["customer_count"])
#         if  queue_length > MIN_QUEUE_THRESHOLD:
#             # Check mandatory minimums before suggesting an open
#             if sid.startswith("RC1") and rc1_stations_open < MIN_OPEN_RC1:
#                 # This branch is primarily for initial startup if a mandatory station isn't logged yet
#                 # However, the core logic should focus on excess queue length.
#                 pass  # Already handled by the overall open/close logic
#
#             # Print command to open another station
#             print(f"COMMAND: Queue at {sid} has {queue_length} people. OPEN another station to balance load.")
#
#             # Exit loop after finding one long queue to avoid multiple open commands
#             return
#
#     # Check for empty stations (CLOSE a station)
#     # Only consider closing if the minimum required stations are still met AFTER closing.
#
#     can_close_rc1 = rc1_stations_open > MIN_OPEN_RC1
#     can_close_sccx = sccx_stations_open > MIN_OPEN_SCCX
#
#     for sid, queue_length in Opened_checkouts_queues.items():
#         if queue_length == 0:
#             # Check if this station is an RC1 and we can close an RC1
#             if sid.startswith("RC1") and can_close_rc1:
#                 # Check if all other *open* stations have manageable queues (<= threshold)
#                 if all(q <= MIN_QUEUE_THRESHOLD for osid, q in Opened_checkouts_queues.items() if osid != sid):
#                     print(f"COMMAND: Station {sid} has 0 queue. All other stations manageable. CLOSE {sid}.")
#                     # Optional: You might remove it from the global dict here if the closure is confirmed
#                     # del Opened_checkouts_queues[sid]
#                     return
#
#             # Check if this station is an SCCx and we can close an SCCx
#             elif sid.startswith("SCCx") and can_close_sccx:
#                 # Check if all other *open* stations have manageable queues (<= threshold)
#                 if all(q <= MIN_QUEUE_THRESHOLD for osid, q in Opened_checkouts_queues.items() if osid != sid):
#                     print(f"COMMAND: Station {sid} has 0 queue. All other stations manageable. CLOSE {sid}.")
#                     # Optional: You might remove it from the global dict here if the closure is confirmed
#                     # del Opened_checkouts_queues[sid]
#                     return
#
#     # Check minimum required stations are up (Startup/Recovery check)
#     if rc1_stations_open < MIN_OPEN_RC1:
#         print(
#             f"ALERT: Only {rc1_stations_open} RC1 stations are open. Need {MIN_OPEN_RC1}. COMMAND: OPEN an RC1 station.")
#         return
#
#     if scc_stations_open < MIN_OPEN_SCCX:
#         print(
#             f"ALERT: Only {sccx_stations_open} SCCx stations are open. Need {MIN_OPEN_SCCX}. COMMAND: OPEN an SCCx station.")
#         return
#
#     # If no conditions were met
#     print("STATUS: Queues are balanced and minimum stations are open. No action required.")



def process_event(event_type,event):
    global Opened_checkouts_queues
    if event_type == "Queue_monitor":
        checkout_update(event) # updates the final count on each cashier.
    else:
        print(f"event type: {event_type}\n")
        print(f"Current counters :{Opened_checkouts_queues}\n")



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

    # for idx, event in enumerate(read_events(args.host, args.port), start=1):
    #     print(f"[{idx}] dataset={event.get('dataset')} sequence={event.get('sequence')}")
    #     if(not event.get('dataset') in event_names):
    #         event_names.append(event.get('dataset'))
    #     print(json.dumps(event.get("event"), indent=2))
    #     print("-")
    #     if args.limit and idx >= args.limit:
    #         break
    #     if idx % 5 ==0:
    #         print(f"Currently identified datasets: {event_names}")

    for idx, event in enumerate(read_events(args.host, args.port), start=1):
        for out in process_event(event):
            print(out)
        if args.limit and idx >= args.limit:
            break


if __name__ == "__main__":
    main()
