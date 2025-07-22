#!/bin/bash

echo "=== BigQuery Emulator Initialization ==="

# Wait for BigQuery emulator to be ready
echo "Waiting for BigQuery emulator to start..."
sleep 10

# Function to check if emulator is ready
check_emulator() {
    curl -s "http://streaming-bigquery:9050/projects/streaming_project/datasets" > /dev/null 2>&1
    return $?
}

# Wait up to 60 seconds for emulator
for i in {1..12}; do
    if check_emulator; then
        echo "✓ BigQuery emulator is ready"
        break
    else
        echo "⏳ Waiting for BigQuery emulator... (attempt $i/12)"
        sleep 5
    fi
    
    if [ $i -eq 12 ]; then
        echo "❌ BigQuery emulator failed to start"
        exit 1
    fi
done

# Check if events table exists
echo "Checking if events table exists..."
TABLE_CHECK=$(curl -s "http://streaming-bigquery:9050/projects/streaming_project/datasets/engagement_data/tables" | grep -o '"tableId":"events"' || echo "")

if [ -z "$TABLE_CHECK" ]; then
    echo "Creating events table..."
    
    RESPONSE=$(curl -s -X POST "http://streaming-bigquery:9050/projects/streaming_project/datasets/engagement_data/tables" \
      -H "Content-Type: application/json" \
      -d '{
        "tableReference": {
          "projectId": "streaming_project",
          "datasetId": "engagement_data", 
          "tableId": "events"
        },
        "schema": {
          "fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "content_id", "type": "STRING"},
            {"name": "user_id", "type": "STRING"},
            {"name": "event_type", "type": "STRING"},
            {"name": "event_ts", "type": "STRING"},
            {"name": "device", "type": "STRING"},
            {"name": "content_type", "type": "STRING"},
            {"name": "duration_ms", "type": "INTEGER"},
            {"name": "engagement_pct", "type": "FLOAT"},
            {"name": "processing_time", "type": "STRING"}
          ]
        }
      }')
    
    if echo "$RESPONSE" | grep -q '"tableId":"events"'; then
        echo "✅ Events table created successfully"
    else
        echo "❌ Failed to create events table"
        echo "Response: $RESPONSE"
        exit 1
    fi
else
    echo "✓ Events table already exists"
fi

echo "✅ BigQuery initialization complete"