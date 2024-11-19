import json
from kafka import KafkaConsumer
from datetime import datetime
import time
KAFKA_BROKER = 'localhost:9092'
LOGGING_TOPIC = 'logs'
HEARTBEAT_TOPIC = 'heartbeats'
NODE_TIMEOUT_SECONDS = 10  # Time in seconds to mark a node as failed
heartbeats = {}


def handle_log_entry(log_entry):
    """
    Handles log entries to identify critical issues (ERROR, WARN).
    """
    level = log_entry.get("log_level", "")
    service = log_entry.get("service_name", "Unknown Service")
    node = log_entry.get("node_id", "Unknown Node")
    time_stamp = log_entry.get("timestamp", "")
    if level in {"ERROR", "WARN"}:
        print(f"[ALERT] Critical Log Found!")
        print(f"  Node ID: {node}")
        print(f"  Service Name: {service}")
        print(f"  Severity Level: {level}")
        print(f"  Time: {time_stamp}")
        print(f"  Message: {log_entry.get('message', '')}")
        if level == "ERROR":
            error_info = log_entry.get("error_details", {})
            print(f"  Error Code: {error_info.get('error_code', '')}")
            print(f"  Error Message: {error_info.get('error_message', '')}")


def handle_heartbeat(heartbeat):
    """
    Processes heartbeat messages to track the status of nodes.
    """
    node = heartbeat.get("node_id", "Unknown Node")
    time_received = datetime.strptime(
        heartbeat.get("timestamp", ""), "%Y-%m-%dT%H:%M:%S")
    heartbeats[node] = time_received


def check_for_node_failures():
    """
    Checks for nodes that have failed based on heartbeat timestamps.
    """
    current_time = datetime.now()
    for node, last_seen in list(heartbeats.items()):
        if (current_time - last_seen).total_seconds() > NODE_TIMEOUT_SECONDS:
            print(f"[ALERT] Node Failure Detected!")
            print(f"  Node ID: {node}")
            print(f"  Last Heartbeat Received: {last_seen}")
            del heartbeats[node]  # Remove the node from monitoring


def run_alert_system():
    """
    Initializes the alerting system for critical logs and node failure detection.
    """
    consumer = KafkaConsumer(
        LOGGING_TOPIC, HEARTBEAT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda msg: json.loads(msg.decode('utf-8'))
    )
    print("Alert system is now active...")
    while True:
        for msg in consumer:
            if msg.topic == LOGGING_TOPIC:
                handle_log_entry(msg.value)
            elif msg.topic == HEARTBEAT_TOPIC:
                handle_heartbeat(msg.value)
        # Perform node failure checks in each loop iteration
        check_for_node_failures()


if __name__ == "__main__":
    try:
        run_alert_system()
    except KeyboardInterrupt:
        print("\nAlert system has been terminated.")
