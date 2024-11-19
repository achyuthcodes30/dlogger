import json
import logging
import smtplib
import configparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import threading
import time

# Configuration
config = configparser.ConfigParser()
config.read('config.ini')
email = config.get('settings', 'email')
password = config.get('settings', 'password')

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO  # Changed to INFO to avoid debug level logs
)

# This will be used for storing node heartbeat information
heartbeat_data = defaultdict(dict)

# Example users list with subscriptions
users = [
    {"email": "shregur@gmail.com", "subscriptions": ["NODE_DED"]},
    {"email": "raoanu2004@gmail.com", "subscriptions": ["NODE_DED"]},
    {"email": "achyuthyogesh0@gmail.com", "subscriptions": ["NODE_DED"]}
]

def send_email(recipient, subject, body):
    message = MIMEMultipart()
    message["From"] = email
    message["To"] = recipient
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(email, password)
            server.sendmail(email, recipient, message.as_string())
        logging.info(f"Email sent to {recipient}!")
    except Exception as e:
        logging.error(f"Error sending email to {recipient}: {e}")

def get_users_subscribed_to_topic(topic):
    return [user['email'] for user in users if topic in user['subscriptions']]

def monitor_heartbeat():
    while True:
        current_time = datetime.utcnow()
        to_alert = []

        for node_id, data in list(heartbeat_data.items()):
            last_heartbeat_time = data.get("last_heartbeat_time")
            if last_heartbeat_time:
                if current_time - last_heartbeat_time > timedelta(seconds=20):
                    logging.warning(f"Node {node_id} did not send a heartbeat in the last 20 seconds.")
                    to_alert.append((node_id, data["service_name"]))
            else:
                logging.error(f"No heartbeat time recorded for node {node_id}, skipping check.")
        
        if to_alert:
            # Send alerts for missing heartbeats
            for node_id, service_name in to_alert:
                subject = f"ALERT: Heartbeat Failure for {service_name}"
                body = f"Dear User,\n\nNo heartbeat received for service '{service_name}' with Node ID '{node_id}' within the last 20 seconds.\n\nPlease check the service immediately."
                subscribed_users = get_users_subscribed_to_topic("NODE_DED")
                for user_email in subscribed_users:
                    send_email(user_email, subject, body)

        # Sleep for 2 seconds before checking again
        time.sleep(2)

def consume_messages():
    consumer = KafkaConsumer(
        'alive', 'alerting',
        bootstrap_servers=['159.223.32.50:9092'],
        group_id='notification-group',
        auto_offset_reset='earliest'
    )

    for message in consumer:
        try:
            message_data = json.loads(message.value.decode('utf-8'))
            topic = message.topic

            if topic == 'alive':
                message_type = message_data.get('message_type')
                node_id = message_data.get('node_id')
                service_name = message_data.get('service_name')

                if message_type == "REGISTRATION":
                    heartbeat_data[node_id] = {
                        "last_heartbeat_time": datetime.utcnow(),
                        "service_name": service_name
                    }
                    logging.info(f"Node {node_id} registered successfully.")

                elif message_type == "HEARTBEAT":
                    if node_id in heartbeat_data:
                        heartbeat_data[node_id]["last_heartbeat_time"] = datetime.utcnow()
                        logging.info(f"Heartbeat received for Node {node_id}.")
                    else:
                        logging.warning(f"Received heartbeat for unregistered Node {node_id}.")

            elif topic == 'alerting':
                print(f"Alerting message received: {message_data}")

        except Exception as e:
            logging.error(f"Error processing message: {e}")

if __name__ == "__main__":
    # Start the heartbeat monitor in a separate thread
    monitor_thread = threading.Thread(target=monitor_heartbeat)
    monitor_thread.daemon = True
    monitor_thread.start()

    # Start consuming messages
    consume_messages()


