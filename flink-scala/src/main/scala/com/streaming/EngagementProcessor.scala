package com.streaming

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.util.Properties
import java.sql.{Connection, DriverManager}
import scala.collection.mutable
import java.time.Instant
import com.streaming.models._
import com.streaming.redis._
import com.streaming.bigquery._

//=========
// JSON Parser Object

object JsonParser extends Serializable {
  @transient lazy val objectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }
  
  def parseDebeziumMessage(json: String): Option[DebeziumMessage] = {
    try {
      Some(objectMapper.readValue(json, classOf[DebeziumMessage]))
    } catch {
      case e: Exception =>
        println(s"Failed to parse JSON: ${e.getMessage}")
        None
    }
  }
}

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
  // Enrichment Functions using DataStream API
  
  def enrichEvent(msg: DebeziumMessage): Option[EnrichedEvent] = {
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
      
      Some(EnrichedEvent(
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
      ))
    } else {
      None
    }
  }
  
  //=========
  // Main with DataStream API
  
  def main(args: Array[String]): Unit = {
    println("Starting Flink Engagement Processor (DataStream API)")
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
    env.enableCheckpointing(10000) // Enable checkpointing for exactly-once processing
    
    // Kafka consumer properties
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    kafkaProps.setProperty("group.id", "flink-datastream-processor")
    kafkaProps.setProperty("auto.offset.reset", "earliest")
    
    // Create Kafka consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "streaming.public.engagement_events",
      new SimpleStringSchema(),
      kafkaProps
    )
    kafkaConsumer.setStartFromEarliest()
    
    // DataStream processing pipeline
    val enrichedStream = env
      .addSource(kafkaConsumer)
      .name("Kafka Source")
      
      // Parse JSON messages
      .map(json => JsonParser.parseDebeziumMessage(json))
      .name("Parse JSON")
      
      // Filter out failed parses
      .filter(_.isDefined)
      .name("Filter Valid Messages")
      
      // Extract parsed messages
      .map(_.get)
      .name("Extract Messages")
      
      // Enrich with content data and calculate metrics
      .map(msg => enrichEvent(msg))
      .name("Enrich Events")
      
      // Filter out non-processable events
      .filter(_.isDefined)
      .name("Filter Enriched Events")
      
      // Extract enriched events
      .map(_.get)
      .name("Extract Enriched Events")
    
    // Output enriched events for monitoring
    enrichedStream
      .map(event => s"Processed: ${event.eventType} | ${event.contentType.getOrElse("?")} | " +
        s"Engagement: ${event.engagementPct.map(p => f"$p%.2f%%").getOrElse("N/A")}")
      .print("DataStream")
    
    // Add Redis sink for analytics
    enrichedStream
      .addSink(new EngagementRedisSink(REDIS_HOST, REDIS_PORT))
      .name("Redis Analytics Sink")

    enrichedStream
      .addSink(new BigQuerySink("streaming_project", "engagement_data", "events", "bigquery-emulator", 9050))
      .name("BigQuery Sink")
    
    // Execute the streaming job
    println("Processing engagement stream with DataStream API...")
    println("-" * 80)
    env.execute("Engagement Stream Processor - DataStream API")
  }
}