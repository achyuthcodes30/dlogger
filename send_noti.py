from kafka import KafkaConsumer
import json
import smtplib
import configparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    {"email": "shregur@gmail.com", "subscriptions": ["LOG", "ERROR"]},
    # {"email": "user2@example.com", "subscriptions": ["LOG", "DEBUG"]},
    # {"email": "user3@example.com", "subscriptions": ["ERROR"]}
]

def get_users_subscribed_to_topic(topic):
    return [user['email'] for user in users if topic in user['subscriptions']]

consumer = KafkaConsumer(
    'LOG', 'ERROR', 'DEBUG',
    bootstrap_servers=['localhost:9092'], 
    group_id='notification-group',
    auto_offset_reset='earliest'
)

for message in consumer:
    try:
        message_data = json.loads(message.value.decode('utf-8'))
        
        topic = message_data.get('message_type')
        log_id = message_data.get('log_id') 
        service_name = message_data.get('service_name') 
        message_content = message_data.get('message')
        
        if topic:
            subscribed_users = get_users_subscribed_to_topic(topic)
            
            for user_email in subscribed_users:
                subject = f"New {topic} Notification: {log_id}"
                body = f"Dear User,\n\nYou have a new update in {topic}:\n\nService: {service_name}\nMessage: {message_content}\n\nLog ID: {log_id}\nTimestamp: {message_data.get('timestamp')}"
                send_email(user_email, subject, body)
    except Exception as e:
        print(f"Error processing message: {e}")

