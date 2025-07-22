package com.streaming.redis

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.configuration.Configuration
import com.streaming.models.EnrichedEvent
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import java.time.Instant

//=========
// Simplified Redis Sink for Testing

class EngagementRedisSink(redisHost: String, redisPort: Int) extends RichSinkFunction[EnrichedEvent] {
  
  @transient private var jedisPool: JedisPool = _
  @transient private var eventCount: Long = 0
  
  override def open(parameters: Configuration): Unit = {
    val config = new JedisPoolConfig()
    config.setMaxTotal(10)
    config.setMaxIdle(5)
    config.setMinIdle(1)
    jedisPool = new JedisPool(config, redisHost, redisPort)
    println(s"Redis sink opened: $redisHost:$redisPort")
  }
  
  override def invoke(event: EnrichedEvent): Unit = {
    eventCount += 1
    
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      
      // Simple counters for testing
      jedis.set("total_events_processed", eventCount.toString)
      jedis.set("last_event_type", event.eventType)
      jedis.set("last_processing_time", Instant.now().toString)
      
      // Content type tracking
      event.contentType.foreach { contentType =>
        jedis.hincrBy("content_type_counts", contentType, 1)
        jedis.set("last_content_type", contentType)
        
        // Engagement tracking
        event.engagementPct.foreach { pct =>
          val timestamp = System.currentTimeMillis()
          jedis.zadd("engagement_scores", pct, s"$contentType-$timestamp")
          
          // Keep only recent scores (last 100)
          val scoreCount = jedis.zcard("engagement_scores")
          if (scoreCount > 100) {
            jedis.zremrangeByRank("engagement_scores", 0, scoreCount - 101)
          }
        }
      }
      
      // Event type tracking
      jedis.hincrBy("event_type_counts", event.eventType, 1)
      
      // Log progress
      if (eventCount % 25 == 0) {
        println(s"Redis: Processed $eventCount events")
      }
      
    } catch {
      case e: Exception =>
        println(s"Redis error for event $eventCount: ${e.getMessage}")
    } finally {
      if (jedis != null) jedis.close()
    }
  }
  
  override def close(): Unit = {
    println(s"Redis sink closing: Processed $eventCount total events")
    if (jedisPool != null) {
      jedisPool.close()
    }
  }
}