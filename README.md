# DLogger

Topic based, distributed log ingestion and retrieval with email alerts and failure detection.
Built as a project under PES University's Big Data course along with [Anurag Rao](https://github.com/anuragrao04) and [Shreya Gurram](https://github.com/bun137).

# Components

- Fluentd
- Kafka
- Logstash
- Elasticsearch
- Email Alerting System

# Prerequisites

- Python
- Kafka
- Fluentd
- Docker

# Setup

- Set up your microservices and push logs (INFO, WARN, ERROR)
- Set up fluentd to aggregate and send logs to the appropriate Kafka topics
- Use Docker compose to setup Logstash and ElasticSearch
- Update the logstash conf to match the Kafka broker IP
- Hit ElasticSearch endpoints to retrieve, search and filter logs
- Spin up the send_noti process (runs a Kafka Consumer) to receive email alerts
