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
import com.streaming.elasticsearch._

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
  
  // Performance Configuration
  val FLINK_PARALLELISM = sys.env.getOrElse("FLINK_PARALLELISM", "12").toInt
  val CHECKPOINT_INTERVAL_MS = sys.env.getOrElse("CHECKPOINT_INTERVAL_MS", "30000").toLong
  val KAFKA_SOURCE_PARALLELISM = sys.env.getOrElse("KAFKA_SOURCE_PARALLELISM", "4").toInt
  val PROCESSING_PARALLELISM = sys.env.getOrElse("PROCESSING_PARALLELISM", "8").toInt
  val REDIS_SINK_PARALLELISM = sys.env.getOrElse("REDIS_SINK_PARALLELISM", "6").toInt
  val BIGQUERY_SINK_PARALLELISM = sys.env.getOrElse("BIGQUERY_SINK_PARALLELISM", "3").toInt
  val ELASTICSEARCH_SINK_PARALLELISM = sys.env.getOrElse("ELASTICSEARCH_SINK_PARALLELISM", "4").toInt
  val MONITORING_SAMPLE_RATE = sys.env.getOrElse("MONITORING_SAMPLE_RATE", "1").toInt
  
  // Kafka Configuration
  val KAFKA_FETCH_MIN_BYTES = sys.env.getOrElse("KAFKA_FETCH_MIN_BYTES", "1048576")
  val KAFKA_FETCH_MAX_WAIT_MS = sys.env.getOrElse("KAFKA_FETCH_MAX_WAIT_MS", "500")
  val KAFKA_MAX_PARTITION_FETCH_BYTES = sys.env.getOrElse("KAFKA_MAX_PARTITION_FETCH_BYTES", "2097152")
  val KAFKA_RECEIVE_BUFFER_BYTES = sys.env.getOrElse("KAFKA_RECEIVE_BUFFER_BYTES", "1048576")
  
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
    
    if (payload.__op == "r" || payload.__op == "c") {
      val contentInfo = contentCache.get(payload.content_id)
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
    println(s"Target Throughput: 2000 events/second")
    
    Thread.sleep(30000)
    loadContentData()
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(FLINK_PARALLELISM)
    env.enableCheckpointing(CHECKPOINT_INTERVAL_MS)
    
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    kafkaProps.setProperty("group.id", "flink-datastream-processor")
    kafkaProps.setProperty("auto.offset.reset", "earliest")
    kafkaProps.setProperty("fetch.min.bytes", KAFKA_FETCH_MIN_BYTES)
    kafkaProps.setProperty("fetch.max.wait.ms", KAFKA_FETCH_MAX_WAIT_MS)
    kafkaProps.setProperty("max.partition.fetch.bytes", KAFKA_MAX_PARTITION_FETCH_BYTES)
    kafkaProps.setProperty("receive.buffer.bytes", KAFKA_RECEIVE_BUFFER_BYTES)
    
    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "streaming.public.engagement_events",
      new SimpleStringSchema(),
      kafkaProps
    )
    kafkaConsumer.setStartFromEarliest()
    
    //=========
    // Processing Pipeline
    
    val enrichedStream = env
      .addSource(kafkaConsumer)
      .name("Kafka Source")
      .setParallelism(KAFKA_SOURCE_PARALLELISM)
      
      .flatMap { json =>
        JsonParser.parseDebeziumMessage(json).flatMap(enrichEvent)
      }
      .name("Parse and Enrich")
      .setParallelism(PROCESSING_PARALLELISM)
    
    //=========
    // Monitoring with Sampling
    
    enrichedStream
      .filter(_ => scala.util.Random.nextInt(100) < MONITORING_SAMPLE_RATE)
      .map(event => s"Processed: ${event.eventType} | ${event.contentType.getOrElse("?")} | " +
        s"Engagement: ${event.engagementPct.map(p => f"$p%.2f%%").getOrElse("N/A")}")
      .print("DataStream")
      .setParallelism(1)
    
    //=========
    // Independent High-Performance Sinks
    
    enrichedStream
      .keyBy(_.contentType.getOrElse("unknown"))
      .addSink(new EngagementRedisSink(REDIS_HOST, REDIS_PORT))
      .name("Redis Analytics Sink")
      .setParallelism(REDIS_SINK_PARALLELISM)

    enrichedStream
      .keyBy(_.userId)
      .addSink(new BigQuerySink("streaming_project", "engagement_data", "events", "bigquery-emulator", 9050))
      .name("BigQuery Sink")
      .setParallelism(BIGQUERY_SINK_PARALLELISM)

    enrichedStream
      .keyBy(_.eventType)
      .addSink(new ElasticsearchSink("elasticsearch", 9200, "engagement-events"))
      .name("Elasticsearch Analytics Sink")
      .setParallelism(ELASTICSEARCH_SINK_PARALLELISM)
    
    println("Processing engagement stream with DataStream API...")
    println("=" * 80)
    println(s"Configuration:")
    println(s"  Total Parallelism: $FLINK_PARALLELISM")
    println(s"  Kafka Source: $KAFKA_SOURCE_PARALLELISM")
    println(s"  Processing: $PROCESSING_PARALLELISM")
    println(s"  Redis Sink: $REDIS_SINK_PARALLELISM")
    println(s"  BigQuery Sink: $BIGQUERY_SINK_PARALLELISM")
    println(s"  Elasticsearch Sink: $ELASTICSEARCH_SINK_PARALLELISM")
    println(s"  Checkpoint Interval: ${CHECKPOINT_INTERVAL_MS}ms")
    println("=" * 80)
    
    env.execute("Engagement Stream Processor - DataStream API")
  }
}