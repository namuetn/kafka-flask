#!/bin/bash

consumer_message="consumer_message.py"
topic_create="topic.py"
bootstrap_servers="172.16.1.29:9092"
topic="$1"
num_consumers=10

if [ -z "$topic" ]; then
    echo "Usage: $0 <topic>"
    exit 1
fi

python3 -u "$topic_create" --bootstrap-servers "$bootstrap_servers" --topic "$topic"
for ((i=1; i<="$num_consumers"; i++)); do
    echo "Running iteration $i"
    nohup python3 -u "$consumer_message" --bootstrap-servers "$bootstrap_servers" --topic "$topic" >> "./logs/output_${topic}.txt" 2>&1 &
done
