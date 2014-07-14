#!/bin/bash

#Example how to run Kafka console-producer

#~/install/kafka/kafka/bin/kafka-console-producer.sh --broker-list ecloud.eanadev.org:9093 --topic uis_logs
~/install/kafka/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic logs
