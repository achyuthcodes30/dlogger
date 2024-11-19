#!/bin/bash

ES_URL="http://localhost:9200"

create_index() {
    local index_name=$1
    curl -X PUT "$ES_URL/$index_name" -H "Content-Type: application/json" -d '{
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "index": {
                "refresh_interval": "1s"
            }
        }
    }'
    echo -e "\nCreated index: $index_name"
}

create_index "error-logs"
create_index "info-logs"
create_index "warn-logs"
create_index "registry"

echo -e "\nVerifying indices:"
curl -X GET "$ES_URL/_cat/indices?v"
