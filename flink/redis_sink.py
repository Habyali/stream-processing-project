import redis
import json
from datetime import datetime, timedelta
from collections import defaultdict
from pyflink.table.udf import udf
from pyflink.table import DataTypes

#=========
# Redis Connection

class RedisEngagementSink:
    def __init__(self, host='redis', port=6379):
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            decode_responses=True
        )
        self.window_minutes = 10
        
    def process_event(self, event_data):
        """Process a single enriched event and update Redis"""
        try:
            # Extract fields
            content_id = event_data.get('content_id')
            content_type = event_data.get('content_type')
            engagement_pct = event_data.get('engagement_pct')
            event_ts = event_data.get('event_ts')
            
            if not all([content_id, content_type]):
                return
            
            # Current timestamp for windowing
            current_time = datetime.now()
            window_key = f"window:{current_time.strftime('%Y%m%d%H%M')}"
            
            #=========
            # Update sliding window data
            
            # 1. Track content type engagement
            if engagement_pct is not None:
                self.redis_client.zadd(
                    f"engagement:by_content_type:{window_key}",
                    {content_type: engagement_pct},
                    incr=True
                )
                
            # 2. Track content access count by type
            self.redis_client.hincrby(
                f"access:by_content_type:{window_key}",
                content_type,
                1
            )
            
            # 3. Track individual content engagement
            if engagement_pct is not None:
                self.redis_client.zadd(
                    f"engagement:by_content:{window_key}",
                    {content_id: engagement_pct}
                )
                
            # 4. Set TTL for auto-cleanup (window + buffer)
            ttl_seconds = (self.window_minutes + 5) * 60
            self.redis_client.expire(f"engagement:by_content_type:{window_key}", ttl_seconds)
            self.redis_client.expire(f"access:by_content_type:{window_key}", ttl_seconds)
            self.redis_client.expire(f"engagement:by_content:{window_key}", ttl_seconds)
            
            #=========
            # Update aggregated metrics
            
            self._update_aggregated_metrics()
            
        except Exception as e:
            print(f"Error processing event in Redis: {e}")
            
    def _update_aggregated_metrics(self):
        """Update aggregated metrics for the last 10 minutes"""
        try:
            current_time = datetime.now()
            
            # Aggregate data from last 10 minutes
            engagement_by_type = defaultdict(float)
            access_by_type = defaultdict(int)
            
            for i in range(self.window_minutes):
                window_time = current_time - timedelta(minutes=i)
                window_key = f"window:{window_time.strftime('%Y%m%d%H%M')}"
                
                # Aggregate engagement scores
                engagement_data = self.redis_client.zrange(
                    f"engagement:by_content_type:{window_key}",
                    0, -1, withscores=True
                )
                for content_type, score in engagement_data:
                    engagement_by_type[content_type] += score
                    
                # Aggregate access counts
                access_data = self.redis_client.hgetall(
                    f"access:by_content_type:{window_key}"
                )
                for content_type, count in access_data.items():
                    access_by_type[content_type] += int(count)
                    
            #=========
            # Store aggregated results
            
            # Top content types by engagement
            self.redis_client.delete("stats:top_content_types_by_engagement")
            for content_type, total_engagement in engagement_by_type.items():
                avg_engagement = total_engagement / max(access_by_type.get(content_type, 1), 1)
                self.redis_client.zadd(
                    "stats:top_content_types_by_engagement",
                    {content_type: avg_engagement}
                )
                
            # Top content types by access count
            self.redis_client.delete("stats:top_content_types_by_access")
            for content_type, count in access_by_type.items():
                self.redis_client.zadd(
                    "stats:top_content_types_by_access",
                    {content_type: count}
                )
                
            # Store last update time
            self.redis_client.set("stats:last_update", current_time.isoformat())
            
        except Exception as e:
            print(f"Error updating aggregated metrics: {e}")
            
    def get_top_content_types(self, limit=2):
        """Get top content types by engagement and access count"""
        try:
            # Top by engagement percentage
            top_by_engagement = self.redis_client.zrevrange(
                "stats:top_content_types_by_engagement",
                0, limit - 1,
                withscores=True
            )
            
            # Top by access count
            top_by_access = self.redis_client.zrevrange(
                "stats:top_content_types_by_access",
                0, limit - 1,
                withscores=True
            )
            
            last_update = self.redis_client.get("stats:last_update")
            
            return {
                "top_by_engagement": [
                    {"content_type": ct, "avg_engagement_pct": round(score, 2)}
                    for ct, score in top_by_engagement
                ],
                "top_by_access": [
                    {"content_type": ct, "access_count": int(score)}
                    for ct, score in top_by_access
                ],
                "last_update": last_update,
                "window_minutes": self.window_minutes
            }
        except Exception as e:
            print(f"Error getting top content types: {e}")
            return None

#=========
# Create UDF for Flink

def create_redis_sink_udf(redis_host='redis', redis_port=6379):
    """Create a UDF that sends data to Redis"""
    sink = RedisEngagementSink(redis_host, redis_port)
    print(f"Redis sink created for {redis_host}:{redis_port}")
    
    @udf(result_type=DataTypes.BOOLEAN())
    def send_to_redis(
        event_id, content_id, user_id, event_type, event_ts, device,
        content_type, length_seconds, duration_ms, engagement_seconds, 
        engagement_pct, processing_time
    ):
        try:
            print(f"Sending to Redis: {content_id}, {content_type}, {engagement_pct}")
            event_data = {
                'event_id': event_id,
                'content_id': content_id,
                'user_id': user_id,
                'event_type': event_type,
                'event_ts': event_ts,
                'device': device,
                'content_type': content_type,
                'length_seconds': length_seconds,
                'duration_ms': duration_ms,
                'engagement_seconds': engagement_seconds,
                'engagement_pct': engagement_pct,
                'processing_time': processing_time
            }
            
            sink.process_event(event_data)
            return True
        except Exception as e:
            print(f"Error in Redis UDF: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    return send_to_redis
