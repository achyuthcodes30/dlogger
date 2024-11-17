import random
import time
import threading
import json
from datetime import datetime

class LoggingService:
    def __init__(self):
        self.node_id = f"1"
        self.service_name = random.choice(["PaymentService", "OrderService", "UserService"])
        self.last_heartbeat = time.time()
        self.log_file = "logs.log" 
        self.lock = threading.Lock()

    def generate_random_log(self):
        log_levels = ["INFO", "WARN", "ERROR"]
        return random.choice(log_levels)

    def generate_log_message(self):
        messages = {
            "INFO": f"Processing request for {self.service_name}",
            "WARN": f"Potential issue detected in {self.service_name}",
            "ERROR": f"Error occurred in {self.service_name}"
        }
        return random.choice(list(messages.values()))

    def generate_heartbeat(self):
        status = "UP" if self.last_heartbeat + 10 < time.time() else "DOWN"
        return {
            "node_id": self.node_id,
            "message_type": "HEARTBEAT",
            "status": status,
            "timestamp": datetime.now().isoformat()
        }

    def send_log(self, log_level, message):
        log = {
            "log_id": f"log_{random.randint(100000, 999999)}",
            "node_id": self.node_id,
            "log_level": log_level,
            "message_type": "LOG",
            "message": message,
            "service_name": self.service_name,
            "timestamp": datetime.now().isoformat()
        }
        # print(json.dumps(log))
        with self.lock:
            with open(self.log_file, "a") as log_file:
                log_file.write(json.dumps(log) + "\n")

    def start_heartbeat(self):
        while True:
            heartbeat = self.generate_heartbeat()
            # print(json.dumps(heartbeat))
            with self.lock:
                with open(self.log_file, "a") as log_file:
                    log_file.write(json.dumps(heartbeat) + "\n")
            self.last_heartbeat = time.time()
            time.sleep(10) 

    def simulate_logging(self):
        while True:
            log_level = self.generate_random_log()
            message = self.generate_log_message()
            self.send_log(log_level, message)
            time.sleep(random.uniform(1, 5)) 

def main():
    logging_service = LoggingService()

    heartbeat_thread = threading.Thread(target=logging_service.start_heartbeat)
    heartbeat_thread.daemon = True 
    heartbeat_thread.start()

    try:
        logging_service.simulate_logging()

    except KeyboardInterrupt:
        print("\nLogging service stopped by user.")
    finally:
        print("Exiting logging service...")

if __name__ == "__main__":
    main()

