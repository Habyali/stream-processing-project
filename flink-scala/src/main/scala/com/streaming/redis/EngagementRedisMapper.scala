package com.streaming.redis

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.configuration.Configuration
import com.streaming.models.EnrichedEvent
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

//=========
// Custom Redis Sink for Engagement Analytics

class EngagementRedisSink(redisHost: String, redisPort: Int) extends RichSinkFunction[EnrichedEvent] {
  
  // Constants
  private val WINDOW_MINUTES = 10
  private val TTL_SECONDS = 900 
  private val UPDATE_STATS_FREQUENCY = 100 
  private val DATE_PATTERN = "yyyyMMddHHmm"
  
  // Redis key prefixes
  private val ACCESS_BY_TYPE_PREFIX = "access:by_type:"
  private val ENGAGEMENT_BY_TYPE_PREFIX = "engagement:by_type:"
  private val WINDOW_PREFIX = "window:"
  private val STATS_TOP_ENGAGEMENT_KEY = "stats:top_by_engagement"
  private val STATS_TOP_ACCESS_KEY = "stats:top_by_access"
  private val STATS_LAST_UPDATE_KEY = "stats:last_update"
  
  // Connection pool settings
  private val POOL_MAX_TOTAL = 10
  private val POOL_MAX_IDLE = 5
  private val POOL_MIN_IDLE = 1
  
  // Redis range indices
  private val ZRANGE_START = 0L
  private val ZRANGE_END = -1L
  
  @transient private var jedisPool: JedisPool = _
  
  override def open(parameters: Configuration): Unit = {
    val config = new JedisPoolConfig()
    config.setMaxTotal(POOL_MAX_TOTAL)
    config.setMaxIdle(POOL_MAX_IDLE)
    config.setMinIdle(POOL_MIN_IDLE)
    jedisPool = new JedisPool(config, redisHost, redisPort)
  }
  
  override def invoke(event: EnrichedEvent): Unit = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      
      // Get current window key
      val now = LocalDateTime.now()
      val dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN)
      val windowKey = s"$WINDOW_PREFIX${now.format(dateFormatter)}"
      
      event.contentType.foreach { contentType =>
        // Update access count
        jedis.hincrBy(s"$ACCESS_BY_TYPE_PREFIX$windowKey", contentType, 1)
        jedis.expire(s"$ACCESS_BY_TYPE_PREFIX$windowKey", TTL_SECONDS)
        
        // Update engagement if available
        event.engagementPct.foreach { pct =>
          jedis.zincrby(s"$ENGAGEMENT_BY_TYPE_PREFIX$windowKey", pct, contentType)
          jedis.expire(s"$ENGAGEMENT_BY_TYPE_PREFIX$windowKey", TTL_SECONDS)
        }
      }
      
      // Update aggregated stats occasionally
      if (true) {
        updateAggregatedStats(jedis, now)
      }
      
    } finally {
      if (jedis != null) jedis.close()
    }
  }
  
  private def updateAggregatedStats(jedis: Jedis, currentTime: LocalDateTime): Unit = {
    val dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN)
    val engagementByType = scala.collection.mutable.Map[String, Double]()
    val accessByType = scala.collection.mutable.Map[String, Int]()
    
    // Aggregate last N minutes
    for (i <- 0 until WINDOW_MINUTES) {
      val windowTime = currentTime.minusMinutes(i)
      val windowKey = s"$WINDOW_PREFIX${windowTime.format(dateFormatter)}"
      
      // Sum engagement scores
      val engagementData = jedis.zrangeWithScores(
        s"$ENGAGEMENT_BY_TYPE_PREFIX$windowKey", 
        ZRANGE_START, 
        ZRANGE_END
      )
      engagementData.asScala.foreach { tuple =>
        val contentType = tuple.getElement
        val score = tuple.getScore
        engagementByType(contentType) = engagementByType.getOrElse(contentType, 0.0) + score
      }
      
      // Sum access counts
      val accessData = jedis.hgetAll(s"$ACCESS_BY_TYPE_PREFIX$windowKey")
      accessData.asScala.foreach { case (contentType, count) =>
        accessByType(contentType) = accessByType.getOrElse(contentType, 0) + count.toInt
      }
    }
    
    // Store aggregated results
    jedis.del(STATS_TOP_ENGAGEMENT_KEY)
    jedis.del(STATS_TOP_ACCESS_KEY)
    
    // Top by average engagement
    engagementByType.foreach { case (contentType, total) =>
      val count = accessByType.getOrElse(contentType, 1)
      val avg = total / count
      jedis.zadd(STATS_TOP_ENGAGEMENT_KEY, avg, contentType)
    }
    
    // Top by access count
    accessByType.foreach { case (contentType, count) =>
      jedis.zadd(STATS_TOP_ACCESS_KEY, count.toDouble, contentType)
    }
    
    jedis.set(STATS_LAST_UPDATE_KEY, currentTime.toString)
    println(s"Updated Redis stats at $currentTime")
  }
  
  override def close(): Unit = {
    if (jedisPool != null) {
      jedisPool.close()
    }
  }
}