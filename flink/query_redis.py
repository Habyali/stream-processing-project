#!/usr/bin/env python3
import redis
import time

def main():
    # Connect to Redis
    r = redis.Redis(host='redis', port=6379, decode_responses=True)
    
    print("=== Redis Engagement Analytics (10-minute window) ===")
    print("")
    
    while True:
        try:
            # Get top content types by engagement
            top_engagement = r.zrevrange("stats:top_by_engagement", 0, 1, withscores=True)
            
            # Get top content types by access count
            top_access = r.zrevrange("stats:top_by_access", 0, 1, withscores=True)
            
            # Get last update time
            last_update = r.get("stats:last_update")
            
            print(f"Last Update: {last_update}")
            print("")
            
            print("Top 2 Content Types by Average Engagement %:")
            for content_type, score in top_engagement:
                print(f"  - {content_type}: {score:.2f}%")
            
            print("")
            print("Top 2 Content Types by Access Count:")
            for content_type, count in top_access:
                print(f"  - {content_type}: {int(count)} accesses")
            
            print("\n" + "-" * 50 + "\n")
            
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()