package com.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.util.Properties
import java.sql.{Connection, DriverManager}
import scala.collection.mutable
import java.time.Instant
import com.streaming.models._
import com.streaming.redis._

object EngagementProcessor {
  
  //=========
  // Configuration
  
  val KAFKA_BOOTSTRAP_SERVERS = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
  val POSTGRES_HOST = sys.env.getOrElse("POSTGRES_HOST", "postgresql")
  val POSTGRES_PORT = sys.env.getOrElse("POSTGRES_PORT", "5432")
  val POSTGRES_DB = sys.env.getOrElse("POSTGRES_DB", "streaming_db")
  val POSTGRES_USER = sys.env.getOrElse("POSTGRES_USER", "streaming_user")
  val POSTGRES_PASSWORD = sys.env.getOrElse("POSTGRES_PASSWORD", "streaming_pass")
  val REDIS_HOST = sys.env.getOrElse("REDIS_HOST", "redis")
  val REDIS_PORT = sys.env.getOrElse("REDIS_PORT", "6379").toInt
  
  //=========
  // Content Cache
  
  val contentCache = mutable.Map[String, ContentInfo]()
  
  def loadContentData(): Unit = {
    println("Loading content data from PostgreSQL...")
    
    val url = s"jdbc:postgresql://$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
    var connection: Connection = null
    
    try {
      connection = DriverManager.getConnection(url, POSTGRES_USER, POSTGRES_PASSWORD)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        "SELECT id::text, content_type, length_seconds FROM content"
      )
      
      while (resultSet.next()) {
        val id = resultSet.getString("id")
        val contentType = resultSet.getString("content_type")
        val lengthSeconds = Option(resultSet.getInt("length_seconds")).filter(_ => !resultSet.wasNull())
        
        contentCache(id) = ContentInfo(id, contentType, lengthSeconds)
      }
      
      println(s"Loaded ${contentCache.size} content records")
    } finally {
      if (connection != null) connection.close()
    }
  }
  
  //=========
  // JSON Parsing
  
  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  def parseDebeziumMessage(json: String): Option[DebeziumMessage] = {
    try {
      Some(objectMapper.readValue(json, classOf[DebeziumMessage]))
    } catch {
      case e: Exception =>
        println(s"Failed to parse JSON: ${e.getMessage}")
        None
    }
  }
  
  //=========
  // Enrichment Process Function
  
  class EnrichmentProcessFunction extends ProcessFunction[DebeziumMessage, EnrichedEvent] {
    
    override def processElement(
      msg: DebeziumMessage,
      ctx: ProcessFunction[DebeziumMessage, EnrichedEvent]#Context,
      out: Collector[EnrichedEvent]
    ): Unit = {
      
      val payload = msg.payload
      
      // Only process insert or read operations
      if (payload.__op == "r" || payload.__op == "c") {
        
        // Lookup content info
        val contentInfo = contentCache.get(payload.content_id)
        
        // Calculate engagement metrics
        val engagementSeconds = payload.duration_ms.map(_ / 1000.0)
        
        val engagementPct = for {
          durMs <- payload.duration_ms
          info <- contentInfo
          lengthSec <- info.lengthSeconds
          if lengthSec > 0
        } yield Math.round((durMs / 1000.0 / lengthSec) * 100 * 100) / 100.0
        
        val enrichedEvent = EnrichedEvent(
          id = payload.id,
          contentId = payload.content_id,
          userId = payload.user_id,
          eventType = payload.event_type,
          eventTs = payload.event_ts,
          device = payload.device,
          contentType = contentInfo.map(_.contentType),
          lengthSeconds = contentInfo.flatMap(_.lengthSeconds),
          durationMs = payload.duration_ms,
          engagementSeconds = engagementSeconds,
          engagementPct = engagementPct,
          processingTime = Instant.now().toString
        )
        
        out.collect(enrichedEvent)
      }
    }
  }
  
  //=========
  // Main
  
  def main(args: Array[String]): Unit = {
    println("Starting Flink Engagement Processor (Scala)")
    println(s"Kafka: $KAFKA_BOOTSTRAP_SERVERS")
    println(s"PostgreSQL: $POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB")
    println(s"Redis: $REDIS_HOST:$REDIS_PORT")
    
    // Wait for services
    println("Waiting for Kafka topic to be ready...")
    Thread.sleep(30000)
    
    // Load content data
    loadContentData()
    
    // Set up Flink environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    // Kafka consumer properties
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    kafkaProps.setProperty("group.id", "flink-scala-processor")
    
    // Create Kafka consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "streaming.public.engagement_events",
      new SimpleStringSchema(),
      kafkaProps
    )
    kafkaConsumer.setStartFromEarliest()
    
    // Process stream
    val enrichedStream = env
      .addSource(kafkaConsumer)
      .map(json => parseDebeziumMessage(json))
      .filter(_.isDefined)
      .map(_.get)
      .process(new EnrichmentProcessFunction)
    
    // Print enriched events
    enrichedStream.print("Enriched")
    
    // Add custom Redis sink for engagement analytics
    enrichedStream.addSink(new EngagementRedisSink(REDIS_HOST, REDIS_PORT))
    
    // Execute
    println("Processing engagement stream with enrichment and Redis sink...")
    println("-" * 80)
    env.execute("Engagement Stream Processor with Redis")
  }
}