#!/bin/bash

echo "=== Stream Processing Project - Distributed Streaming Pipeline ==="
echo ""



reset_database() {
    echo ""
    echo "🔄 Resetting database..."
    
    # First, drop and recreate the Debezium publication to clear locks
    echo "Clearing Debezium locks..."
    docker-compose exec -T postgresql psql -U streaming_user -d streaming_db -c \
        "DROP PUBLICATION IF EXISTS dbz_publication;" 2>/dev/null
    
    # Reset engagement_events first (child table)
    echo "Truncating engagement_events table..."
    docker-compose exec -T postgresql psql -U streaming_user -d streaming_db -c \
        "DELETE FROM engagement_events;" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "✅ engagement_events table reset successfully"
    else
        echo "❌ Failed to reset engagement_events table"
        return 1
    fi
    
    # Reset content table (parent table)
    echo "Truncating content table..."
    docker-compose exec -T postgresql psql -U streaming_user -d streaming_db -c \
        "DELETE FROM content;" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "✅ content table reset successfully"
    else
        echo "❌ Failed to reset content table"
        return 1
    fi
    
    # Reset sequences
    echo "Resetting sequences..."
    docker-compose exec -T postgresql psql -U streaming_user -d streaming_db -c \
        "ALTER SEQUENCE engagement_events_id_seq RESTART WITH 1;" 2>/dev/null
    
    # Recreate publication for Debezium
    echo "Recreating Debezium publication..."
    docker-compose exec -T postgresql psql -U streaming_user -d streaming_db -c \
        "CREATE PUBLICATION dbz_publication FOR TABLE engagement_events;" 2>/dev/null
    
    echo "✅ Database reset completed"
}

# Function to check if resetdb flag is present
check_reset_flag() {
    echo "DEBUG: Checking arguments: $@"
    for arg in "$@"; do
        echo "DEBUG: Found argument: $arg"
        if [ "$arg" = "--resetdb" ]; then
            echo "DEBUG: Reset flag found!"
            return 0
        fi
    done
    echo "DEBUG: No reset flag found"
    return 1
}

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
        # Check for reset flag FIRST
        if check_reset_flag "$@"; then
            echo "🔄 Reset flag detected, starting with database reset..."
            RESET_AFTER_START=true
        fi
        
        echo "Starting services..."
        docker-compose up -d --build
        
        echo ""
        echo "Waiting for PostgreSQL to be ready..."
        sleep 10
        
        # Reset database IMMEDIATELY after PostgreSQL is ready, BEFORE generator runs
        if [ "$RESET_AFTER_START" = true ]; then
            echo "🔄 Resetting database before any data generation..."
            
            # Stop generator first to prevent it from creating content
            docker-compose stop generator
            
            # Reset database
            reset_database
            
            # Now start fresh generator with current .env
            echo "🔄 Starting fresh generator with current .env values..."
            docker-compose rm -f generator
            docker-compose up -d --build generator
            
        fi
        
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
        sleep 3

        pip install -r requirements.txt >/dev/null 2>&1
        chmod +x monitor.py
        python3 monitor.py
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
        ./run.sh start "$@"
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
        echo "Usage: ./run.sh {start|stop|restart|logs|test|clean} [--resetdb]"
        echo ""
        echo "Options:"
        echo "  start [--resetdb]  Start services (optionally reset database first)"
        echo "  stop               Stop all services"
        echo "  restart [--resetdb] Restart services"
        echo "  logs               View service logs"
        echo "  test               Test service status"
        echo "  clean              Stop services and remove volumes"
        echo ""
        echo "Flags:"
        echo "  --resetdb          Reset database tables before starting"
        exit 1
        ;;
esac