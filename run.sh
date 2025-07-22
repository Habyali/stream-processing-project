#!/bin/bash

echo "=== Stream Processing Project - Distributed Streaming Pipeline ==="
echo ""

# Function to check if service is running
check_service() {
    if docker-compose ps | grep -q "$1.*Up"; then
        echo "✓ $1 is running"
        return 0
    else
        echo "✗ $1 is not running"
        return 1
    fi
}


# Function to test PostgreSQL connection
test_postgres() {
    echo ""
    echo "Testing PostgreSQL connection..."
    docker-compose exec -T postgresql psql -U streaming_user -d streaming_db -c "\dt" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "✓ PostgreSQL connection successful"
        
        # Check row counts
        echo ""
        echo "Row counts:"
        docker-compose exec -T postgresql psql -U streaming_user -d streaming_db -c \
            "SELECT 'content' as table_name, COUNT(*) as row_count FROM content 
             UNION ALL 
             SELECT 'engagement_events', COUNT(*) FROM engagement_events;" 2>/dev/null
    else
        echo "✗ PostgreSQL connection failed"
    fi
}

# Function to check Kafka topics
check_kafka_topics() {
    echo ""
    echo "Kafka topics:"
    docker-compose exec -T kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -E "(streaming|connect)" || echo "No streaming topics found yet"
}

# Function to check BigQuery
check_bigquery() {
    echo ""
    echo "BigQuery emulator status:"
    if curl -s "http://localhost:9050/projects/streaming_project/datasets" >/dev/null 2>&1; then
        echo "✓ BigQuery emulator accessible"
        
        # Check table exists
        TABLE_CHECK=$(curl -s "http://localhost:9050/projects/streaming_project/datasets/engagement_data/tables" | grep -o '"tableId":"events"' || echo "")
        if [ -n "$TABLE_CHECK" ]; then
            echo "✓ Events table exists"
            
            # Check row count
            QUERY_RESULT=$(curl -s -X POST "http://localhost:9050/projects/streaming_project/queries" \
              -H "Content-Type: application/json" \
              -d '{"query": "SELECT COUNT(*) as total_events FROM engagement_data.events"}' | grep -o '"v":"[0-9]*"' | head -1 | cut -d'"' -f4)
            
            if [ -n "$QUERY_RESULT" ]; then
                echo "✓ Events in BigQuery: $QUERY_RESULT"
            fi
        else
            echo "⚠ Events table not found"
        fi
    else
        echo "✗ BigQuery emulator not accessible"
    fi
}

# Function to create BigQuery table if not exists
create_bigquery_table() {
    echo ""
    echo "Ensuring BigQuery events table exists..."
    
    # Check if table exists first
    TABLE_CHECK=$(curl -s "http://localhost:9050/projects/streaming_project/datasets/engagement_data/tables" | grep -o '"tableId":"events"' || echo "")
    
    if [ -z "$TABLE_CHECK" ]; then
        echo "Creating events table..."
        
        RESPONSE=$(curl -s -X POST "http://localhost:9050/projects/streaming_project/datasets/engagement_data/tables" \
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
        fi
    else
        echo "✓ Events table already exists"
    fi
}


# Main execution
case "${1:-start}" in
    start)
        echo "Starting services..."
        docker-compose up -d --build
        
        echo ""
        echo "Waiting for services to be ready..."
        sleep 10
        
        check_service "streaming-postgres"
        check_service "streaming-generator"
        check_service "streaming-zookeeper"
        check_service "streaming-kafka"
        check_service "streaming-connect"
        check_service "streaming-flink-jobmanager"
        check_service "streaming-flink-taskmanager"
        check_service "streaming-flink-processor"
        check_service "streaming-redis"
        check_service "streaming-bigquery"
        check_bigquery
        create_bigquery_table
        
        test_postgres
        check_kafka_topics
        
        echo ""
        echo "Setting up Debezium connector..."
        sleep 5
        chmod +x kafka/setup-connector.sh
        ./kafka/setup-connector.sh
        
        echo ""
        echo "To view logs: docker-compose logs -f"
        echo "To stop: ./run.sh stop"
        ;;
        
    stop)
        echo "Stopping services..."
        docker-compose down
        ;;
        
    clean)
        echo "Cleaning up services and volumes..."
        docker-compose down -v
        ;;
        
    restart)
        ./run.sh stop
        sleep 2
        ./run.sh start
        ;;
        
    logs)
        docker-compose logs -f
        ;;
        
    test)
        check_service "streaming-postgres"
        check_service "streaming-generator"
        check_service "streaming-zookeeper"
        check_service "streaming-kafka"
        check_service "streaming-connect"
        test_postgres
        check_kafka_topics
        check_bigquery
        ;;
        
    *)
        echo "Usage: ./run.sh {start|stop|restart|logs|test|clean}"
        exit 1
        ;;
esac