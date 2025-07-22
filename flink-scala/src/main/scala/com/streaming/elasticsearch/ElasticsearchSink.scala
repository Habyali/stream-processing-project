package com.streaming.elasticsearch

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.configuration.Configuration
import com.streaming.models.EnrichedEvent
import java.io.{OutputStreamWriter, BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

//=========
// Elasticsearch Sink for High Throughput

class ElasticsearchSink(
  elasticsearchHost: String,
  elasticsearchPort: Int,
  indexName: String
) extends RichSinkFunction[EnrichedEvent] {

  //=========
  // Configuration Constants
  
  private val BATCH_SIZE = sys.env.getOrElse("ELASTICSEARCH_BATCH_SIZE", "1000").toInt
  private val FLUSH_INTERVAL_MS = sys.env.getOrElse("ELASTICSEARCH_FLUSH_INTERVAL_MS", "10000").toLong
  private val MAX_BUFFER_SIZE = sys.env.getOrElse("ELASTICSEARCH_MAX_BUFFER_SIZE", "10000").toInt
  private val CONNECTION_TIMEOUT_MS = sys.env.getOrElse("ELASTICSEARCH_CONNECTION_TIMEOUT_MS", "5000").toInt
  private val READ_TIMEOUT_MS = sys.env.getOrElse("ELASTICSEARCH_READ_TIMEOUT_MS", "15000").toInt
  private val INDEX_SHARDS = sys.env.getOrElse("ELASTICSEARCH_INDEX_SHARDS", "1").toInt
  private val INDEX_REPLICAS = sys.env.getOrElse("ELASTICSEARCH_INDEX_REPLICAS", "0").toInt
  
  //=========
  // Instance Variables
  
  @transient private var eventBuffer: ArrayBuffer[EnrichedEvent] = _
  @transient private var lastFlushTime: Long = _
  @transient private var eventCount: Long = _
  @transient private var batchCount: Long = _
  @transient private var executor: ScheduledExecutorService = _

  override def open(parameters: Configuration): Unit = {
    eventBuffer = ArrayBuffer[EnrichedEvent]()
    lastFlushTime = System.currentTimeMillis()
    eventCount = 0
    batchCount = 0
    executor = Executors.newScheduledThreadPool(2)
    
    executor.scheduleAtFixedRate(
      new Runnable { def run(): Unit = flushBuffer(force = false) },
      FLUSH_INTERVAL_MS,
      FLUSH_INTERVAL_MS,
      TimeUnit.MILLISECONDS
    )
    
    println(s"Elasticsearch Sink opened - Target: $elasticsearchHost:$elasticsearchPort/$indexName")
    println(s"Batch size: $BATCH_SIZE, flush interval: ${FLUSH_INTERVAL_MS}ms")
    
    testConnectionAndCreateTemplate()
  }

  private def testConnectionAndCreateTemplate(): Unit = {
    try {
      val healthUrl = new URL(s"http://$elasticsearchHost:$elasticsearchPort/_cluster/health")
      val healthConnection = healthUrl.openConnection().asInstanceOf[HttpURLConnection]
      healthConnection.setRequestMethod("GET")
      healthConnection.setConnectTimeout(CONNECTION_TIMEOUT_MS)
      healthConnection.setReadTimeout(READ_TIMEOUT_MS)
      
      val healthCode = healthConnection.getResponseCode
      if (healthCode == 200) {
        println("✓ Elasticsearch connection successful")
        createIndexTemplate()
      } else {
        println(s"⚠ Elasticsearch health check returned: $healthCode")
      }
      healthConnection.disconnect()
      
    } catch {
      case e: Exception =>
        println(s"⚠ Elasticsearch connection failed: ${e.getMessage}")
        println("Will continue with batching to files")
    }
  }

  private def createIndexTemplate(): Unit = {
    try {
      val templateUrl = new URL(s"http://$elasticsearchHost:$elasticsearchPort/_index_template/engagement_events_template")
      val connection = templateUrl.openConnection().asInstanceOf[HttpURLConnection]
      
      connection.setRequestMethod("PUT")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setDoOutput(true)
      
      val template = s"""{
        "index_patterns": ["$indexName-*"],
        "template": {
          "settings": {
            "number_of_shards": $INDEX_SHARDS,
            "number_of_replicas": $INDEX_REPLICAS
          },
          "mappings": {
            "properties": {
              "id": {"type": "long"},
              "content_id": {"type": "keyword"},
              "user_id": {"type": "keyword"},
              "event_type": {"type": "keyword"},
              "event_timestamp": {"type": "date"},
              "device": {"type": "keyword"},
              "content_type": {"type": "keyword"},
              "duration_ms": {"type": "long"},
              "engagement_pct": {"type": "float"},
              "processing_time": {"type": "date"},
              "@timestamp": {"type": "date"}
            }
          }
        }
      }"""
      
      val writer = new OutputStreamWriter(connection.getOutputStream, "UTF-8")
      writer.write(template)
      writer.flush()
      writer.close()
      
      val responseCode = connection.getResponseCode
      if (responseCode >= 200 && responseCode < 300) {
        println("✓ Elasticsearch index template created")
      } else {
        println(s"⚠ Index template creation returned: $responseCode")
      }
      connection.disconnect()
      
    } catch {
      case e: Exception =>
        println(s"⚠ Failed to create index template: ${e.getMessage}")
    }
  }

  override def invoke(event: EnrichedEvent): Unit = {
    synchronized {
      if (eventBuffer.size >= MAX_BUFFER_SIZE) {
        return
      }
      
      eventCount += 1
      eventBuffer += event
      
      if (eventCount <= 5 || eventCount % 100 == 0) {
        println(s"Elasticsearch buffered event #$eventCount: ${event.eventType} | ${event.contentType.getOrElse("N/A")}")
      }

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
        def run(): Unit = flushToElasticsearch(eventsToProcess)
      })
    }
  }

  private def flushToElasticsearch(eventsToSend: List[EnrichedEvent]): Unit = {
    batchCount += 1

    try {
      val today = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      val indexWithDate = s"$indexName-$today"
      
      val bulkPayload = createBulkPayload(eventsToSend, indexWithDate)
      
      if (!sendBulkRequest(bulkPayload)) {
        writeToFile(eventsToSend)
      }
      
      println(s"Elasticsearch: Processed batch #$batchCount with ${eventsToSend.size} events")
      
    } catch {
      case e: Exception =>
        println(s"Elasticsearch batch #$batchCount error: ${e.getMessage}")
        writeToFile(eventsToSend)
    }
  }

  private def createBulkPayload(events: List[EnrichedEvent], indexName: String): String = {
    val bulkLines = events.flatMap { event =>
      val indexLine = s"""{"index":{"_index":"$indexName"}}"""
      val documentLine = convertEventToDocument(event)
      List(indexLine, documentLine)
    }
    bulkLines.mkString("\n") + "\n"
  }

  private def convertEventToDocument(event: EnrichedEvent): String = {
    val eventTimestamp = formatTimestamp(event.eventTs)
    val processingTimestamp = formatTimestamp(event.processingTime)
    val currentTimestamp = LocalDateTime.now().atOffset(ZoneOffset.UTC).toString
    
    s"""{
      |  "id": ${event.id},
      |  "content_id": "${event.contentId}",
      |  "user_id": "${event.userId}",
      |  "event_type": "${event.eventType}",
      |  "event_timestamp": "$eventTimestamp",
      |  "device": "${event.device}",
      |  "content_type": ${event.contentType.map(ct => s""""$ct"""").getOrElse("null")},
      |  "duration_ms": ${event.durationMs.getOrElse("null")},
      |  "engagement_pct": ${event.engagementPct.getOrElse("null")},
      |  "processing_time": "$processingTimestamp",
      |  "@timestamp": "$currentTimestamp"
      |}""".stripMargin.replaceAll("\n", "")
  }

  private def formatTimestamp(timestamp: String): String = {
    try {
      val cleanTs = timestamp.replace("Z", "").replace("T", " ")
      val dateTime = if (cleanTs.contains(".")) {
        LocalDateTime.parse(cleanTs.substring(0, cleanTs.indexOf(".")), 
                          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      } else {
        LocalDateTime.parse(cleanTs, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      }
      dateTime.atOffset(ZoneOffset.UTC).toString
    } catch {
      case _: Exception => 
        LocalDateTime.now().atOffset(ZoneOffset.UTC).toString
    }
  }

  private def sendBulkRequest(bulkPayload: String): Boolean = {
    try {
      val url = new URL(s"http://$elasticsearchHost:$elasticsearchPort/_bulk")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/x-ndjson")
      connection.setDoOutput(true)
      connection.setConnectTimeout(CONNECTION_TIMEOUT_MS)
      connection.setReadTimeout(READ_TIMEOUT_MS)
      
      val writer = new OutputStreamWriter(connection.getOutputStream, "UTF-8")
      writer.write(bulkPayload)
      writer.flush()
      writer.close()
      
      val responseCode = connection.getResponseCode
      
      if (responseCode >= 200 && responseCode < 300) {
        println(s"✓ Elasticsearch bulk insert successful for batch #$batchCount")
        true
      } else {
        val reader = new BufferedReader(new InputStreamReader(connection.getErrorStream))
        val errorResponse = reader.readLine()
        reader.close()
        
        println(s"⚠ Elasticsearch bulk insert failed with code: $responseCode")
        println(s"⚠ Error: $errorResponse")
        false
      }
    } catch {
      case e: Exception =>
        println(s"⚠ Elasticsearch HTTP error: ${e.getMessage}")
        false
    }
  }

  private def writeToFile(events: List[EnrichedEvent]): Unit = {
    try {
      val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
      val filename = s"/tmp/elasticsearch_batch_${timestamp}_${batchCount}.json"
      val writer = new java.io.BufferedWriter(new java.io.FileWriter(filename))

      events.foreach { event =>
        val jsonEvent = convertEventToDocument(event)
        writer.write(jsonEvent)
        writer.newLine()
      }

      writer.close()
      println(s"✓ Elasticsearch: Wrote ${events.size} events to $filename")
      
    } catch {
      case e: Exception =>
        println(s"✗ Elasticsearch file write error: ${e.getMessage}")
    }
  }

  override def close(): Unit = {
    flushBuffer(force = true)
    
    if (executor != null) {
      executor.shutdown()
      try {
        executor.awaitTermination(10, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException => executor.shutdownNow()
      }
    }
    
    println(s"Elasticsearch Sink closing - processed $eventCount events in $batchCount batches")
  }
}