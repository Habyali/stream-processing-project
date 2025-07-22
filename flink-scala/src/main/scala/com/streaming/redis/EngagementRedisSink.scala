package com.streaming.redis

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.configuration.Configuration
import com.streaming.models.EnrichedEvent
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

//=========
// Redis Sink for High Throughput

class EngagementRedisSink(redisHost: String, redisPort: Int) extends RichSinkFunction[EnrichedEvent] {
  
  //=========
  // Configuration Constants
  
  private val WINDOW_MINUTES = 10
  private val TTL_SECONDS = 900 
  private val UPDATE_STATS_FREQUENCY = sys.env.getOrElse("REDIS_UPDATE_STATS_FREQUENCY", "10").toInt 
  private val DATE_PATTERN = "yyyyMMddHHmm"
  private val BATCH_SIZE = sys.env.getOrElse("REDIS_BATCH_SIZE", "500").toInt
  private val FLUSH_INTERVAL_MS = sys.env.getOrElse("REDIS_FLUSH_INTERVAL_MS", "2000").toLong
  private val MAX_BUFFER_SIZE = sys.env.getOrElse("REDIS_MAX_BUFFER_SIZE", "5000").toInt
  
  //=========
  // Redis Key Prefixes
  
  private val ACCESS_BY_TYPE_PREFIX = "access:by_type:"
  private val ENGAGEMENT_BY_TYPE_PREFIX = "engagement:by_type:"
  private val WINDOW_PREFIX = "window:"
  private val STATS_TOP_ENGAGEMENT_KEY = "stats:top_by_engagement"
  private val STATS_TOP_ACCESS_KEY = "stats:top_by_access"
  private val STATS_LAST_UPDATE_KEY = "stats:last_update"
  
  //=========
  // Connection Pool Settings
  
  private val POOL_MAX_TOTAL = sys.env.getOrElse("REDIS_POOL_MAX_TOTAL", "20").toInt
  private val POOL_MAX_IDLE = sys.env.getOrElse("REDIS_POOL_MAX_IDLE", "10").toInt
  private val POOL_MIN_IDLE = sys.env.getOrElse("REDIS_POOL_MIN_IDLE", "2").toInt
  private val POOL_TIMEOUT_MS = sys.env.getOrElse("REDIS_POOL_TIMEOUT_MS", "5000").toInt
  
  //=========
  // Redis Range Indices
  
  private val ZRANGE_START = 0L
  private val ZRANGE_END = -1L
  
  //=========
  // Instance Variables
  
  @transient private var jedisPool: JedisPool = _
  @transient private var eventBuffer: ArrayBuffer[EnrichedEvent] = _
  @transient private var lastFlushTime: Long = _
  @transient private var executor: ScheduledExecutorService = _
  @transient private var eventCount: Long = _
  
  override def open(parameters: Configuration): Unit = {
    val config = new JedisPoolConfig()
    config.setMaxTotal(POOL_MAX_TOTAL)
    config.setMaxIdle(POOL_MAX_IDLE)
    config.setMinIdle(POOL_MIN_IDLE)
    config.setTestOnBorrow(true)
    config.setTestOnReturn(true)
    config.setTestWhileIdle(true)
    config.setBlockWhenExhausted(true)
    config.setMaxWaitMillis(POOL_TIMEOUT_MS)
    
    jedisPool = new JedisPool(config, redisHost, redisPort)
    eventBuffer = ArrayBuffer[EnrichedEvent]()
    lastFlushTime = System.currentTimeMillis()
    executor = Executors.newScheduledThreadPool(2)
    eventCount = 0
    
    executor.scheduleAtFixedRate(
      new Runnable { def run(): Unit = flushBuffer(force = false) },
      FLUSH_INTERVAL_MS,
      FLUSH_INTERVAL_MS,
      TimeUnit.MILLISECONDS
    )
    
    println(s"Redis Sink opened with batch size: $BATCH_SIZE, flush interval: ${FLUSH_INTERVAL_MS}ms")
  }
  
  override def invoke(event: EnrichedEvent): Unit = {
    synchronized {
      if (eventBuffer.size >= MAX_BUFFER_SIZE) {
        return
      }
      
      eventBuffer += event
      eventCount += 1
      
      if (eventBuffer.size >= BATCH_SIZE) {
        flushBuffer(force = true)
      }
    }
  }
  
  private def flushBuffer(force: Boolean): Unit = {
    val eventsToProcess = synchronized {
      val shouldFlush = force || 
        (System.currentTimeMillis() - lastFlushTime) > FLUSH_INTERVAL_MS
      
      if (shouldFlush && eventBuffer.nonEmpty) {
        val events = eventBuffer.toList
        eventBuffer.clear()
        lastFlushTime = System.currentTimeMillis()
        events
      } else {
        List.empty[EnrichedEvent]
      }
    }
    
    if (eventsToProcess.nonEmpty) {
      executor.submit(new Runnable {
        def run(): Unit = processBatch(eventsToProcess)
      })
    }
  }
  
  private def processBatch(events: List[EnrichedEvent]): Unit = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      
      val now = LocalDateTime.now()
      val dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN)
      val windowKey = s"$WINDOW_PREFIX${now.format(dateFormatter)}"
      
      events.foreach { event =>
        event.contentType.foreach { contentType =>
          pipeline.hincrBy(s"$ACCESS_BY_TYPE_PREFIX$windowKey", contentType, 1)
          pipeline.expire(s"$ACCESS_BY_TYPE_PREFIX$windowKey", TTL_SECONDS)
          
          event.engagementPct.foreach { pct =>
            pipeline.zincrby(s"$ENGAGEMENT_BY_TYPE_PREFIX$windowKey", pct, contentType)
            pipeline.expire(s"$ENGAGEMENT_BY_TYPE_PREFIX$windowKey", TTL_SECONDS)
          }
        }
      }
      
      pipeline.sync()
      
      updateAggregatedStats(jedis, now)
      
    } catch {
      case e: Exception =>
        println(s"Redis batch processing failed: ${e.getMessage}")
    } finally {
      if (jedis != null) jedis.close()
    }
  }
  
  private def updateAggregatedStats(jedis: Jedis, currentTime: LocalDateTime): Unit = {
    val dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN)
    val engagementByType = scala.collection.mutable.Map[String, Double]()
    val accessByType = scala.collection.mutable.Map[String, Int]()
    
    for (i <- 0 until WINDOW_MINUTES) {
      val windowTime = currentTime.minusMinutes(i)
      val windowKey = s"$WINDOW_PREFIX${windowTime.format(dateFormatter)}"
      
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
      
      val accessData = jedis.hgetAll(s"$ACCESS_BY_TYPE_PREFIX$windowKey")
      accessData.asScala.foreach { case (contentType, count) =>
        accessByType(contentType) = accessByType.getOrElse(contentType, 0) + count.toInt
      }
    }
    
    jedis.del(STATS_TOP_ENGAGEMENT_KEY)
    jedis.del(STATS_TOP_ACCESS_KEY)
    
    engagementByType.foreach { case (contentType, total) =>
      val count = accessByType.getOrElse(contentType, 1)
      val avg = total / count
      jedis.zadd(STATS_TOP_ENGAGEMENT_KEY, avg, contentType)
    }
    
    accessByType.foreach { case (contentType, count) =>
      jedis.zadd(STATS_TOP_ACCESS_KEY, count.toDouble, contentType)
    }
    
    jedis.set(STATS_LAST_UPDATE_KEY, currentTime.toString)
    println(s"Updated Redis stats at $currentTime")
  }
  
  override def close(): Unit = {
    flushBuffer(force = true)
    
    if (executor != null) {
      executor.shutdown()
      try {
        executor.awaitTermination(5, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException => executor.shutdownNow()
      }
    }
    
    if (jedisPool != null) {
      jedisPool.close()
    }
    
    println(s"Redis Sink closed - processed $eventCount events")
  }
}