#!/bin/bash

echo "=== Stream Processing Project - Generator -> PostgreSQL ==="
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

# Main execution
case "${1:-start}" in
    start)
        echo "Starting services..."
        docker-compose up -d --build
        
        echo ""
        echo "Waiting for services to be ready..."
        sleep 5
        
        # Show PostgreSQL logs if it fails
        if ! docker ps | grep -q "streaming-postgres"; then
            echo ""
            echo "PostgreSQL failed to start. Showing logs:"
            docker-compose logs postgresql
        fi
        
        check_service "streaming-postgres"
        check_service "streaming-generator"
        
        test_postgres
        
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
        test_postgres
        ;;
        
    *)
        echo "Usage: ./run.sh {start|stop|restart|logs|test|clean}"
        exit 1
        ;;
esac