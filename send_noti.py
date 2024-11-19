import asyncio
import json
import smtplib
import configparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta


config = configparser.ConfigParser()
config.read('config.ini')
email = config.get('settings', 'email')
password = config.get('settings', 'password')


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
        print(f"Email sent to {recipient}!")
    except Exception as e:
        print(f"Error sending email to {recipient}: {e}")


users = [
    {"email": "shregur@gmail.com", "subscriptions": ["NODE_DED"]},
    {"email": "raoanu2004@gmail.com",
        "subscriptions": ["NODE_DED"]},
        {"email": "achyuthyogesh0@gmail.com", "subscriptions": ["NODE_DED"]}
]


def get_users_subscribed_to_topic(topic):
    return [user['email'] for user in users if topic in user['subscriptions']]


heartbeat_data = defaultdict(dict)


async def monitor_heartbeat():
    while True:
        current_time = datetime.utcnow()
        to_alert = []

        for node_id, data in list(heartbeat_data.items()):
            last_heartbeat_time = data.get("last_heartbeat_time")
            if last_heartbeat_time and (current_time - last_heartbeat_time > timedelta(seconds=30)):
                # Heartbeat missed; trigger an alert
                to_alert.append((node_id, data["service_name"]))
                del heartbeat_data[node_id]  # Remove the node from tracking

        # Send alerts for missing heartbeats
                for node_id, service_name in to_alert:
                  subscribed_users = get_users_subscribed_to_topic("NODE_DED")
                  subject = f"ALERT: Heartbeat Failure for {service_name}"
                  print(subject)
                  to_alert = []
                  body = f"Dear User,\n\nNo heartbeat received for service '{service_name}' with Node ID '{node_id}' within the last 30 seconds.\n\nPlease check the service immediately."
                  for user_email in subscribed_users:
                    send_email(user_email, subject, body)
            

        await asyncio.sleep(5)  # Check for ded nodes every 5 seconds


# Kafka Consumer configuration
consumer = KafkaConsumer(
    'alive', 'alerting',
    bootstrap_servers=['159.223.32.50:9092'],
    group_id='notification-group',
    auto_offset_reset='earliest'
)

# Start the asyncio loop


async def main():
    asyncio.create_task(monitor_heartbeat())

    for message in consumer:
        try:
            message_data = json.loads(message.value.decode('utf-8'))
            topic = message.topic

            if topic == 'alive':
                message_type = message_data.get('message_type')
                node_id = message_data.get('node_id')
                service_name = message_data.get('service_name')

                if message_type == "REGISTRATION":
                    print(
                        f"Received registration for Node ID: {node_id}, Service: {service_name}")
                    heartbeat_data[node_id] = {
                        "last_heartbeat_time": datetime.utcnow(), "service_name": service_name}

                elif message_type == "HEARTBEAT":
                    print(f"Received heartbeat for Node ID: {node_id}")
                    if node_id in heartbeat_data:
                        heartbeat_data[node_id]["last_heartbeat_time"] = datetime.utcnow(
                        )

            elif topic == 'alerting':
                print(f"Alerting message received: {message_data}")

        except Exception as e:
            print(f"Error processing message: {e}")


# Run the main asyncio loop
asyncio.run(main())
