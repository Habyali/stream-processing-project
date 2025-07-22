#!/bin/bash

echo "Waiting for Kafka Connect to be ready..."
until curl -s http://localhost:8083/connectors > /dev/null; do
    sleep 5
done

echo "Creating Debezium PostgreSQL connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-connector.json

echo ""
echo "Checking connector status..."
sleep 3
curl -s http://localhost:8083/connectors/postgres-connector/status | jq .