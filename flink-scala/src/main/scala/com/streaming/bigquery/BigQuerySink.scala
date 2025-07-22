package com.streaming.bigquery

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.configuration.Configuration
import com.streaming.models.EnrichedEvent
import java.io.{BufferedWriter, FileWriter, OutputStreamWriter}
import java.net.{HttpURLConnection, URL}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

//=========
// Optimized BigQuery Sink for High Throughput

class BigQuerySink(
  projectId: String,
  datasetId: String, 
  tableId: String,
  emulatorHost: String,
  emulatorPort: Int
) extends RichSinkFunction[EnrichedEvent] {

  //=========
  // Configuration Constants
  
  private val BATCH_SIZE = sys.env.getOrElse("BIGQUERY_BATCH_SIZE", "2000").toInt
  private val FLUSH_INTERVAL_MS = sys.env.getOrElse("BIGQUERY_FLUSH_INTERVAL_MS", "30000").toLong
  private val MAX_BUFFER_SIZE = sys.env.getOrElse("BIGQUERY_MAX_BUFFER_SIZE", "20000").toInt
  private val CONNECTION_TIMEOUT_MS = sys.env.getOrElse("BIGQUERY_CONNECTION_TIMEOUT_MS", "5000").toInt
  private val READ_TIMEOUT_MS = sys.env.getOrElse("BIGQUERY_READ_TIMEOUT_MS", "30000").toInt
  
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
    
    println(s"BigQuery Sink opened - Target: $projectId.$datasetId.$tableId")
    println(s"Emulator: $emulatorHost:$emulatorPort")
    println(s"Batch size: $BATCH_SIZE, flush interval: ${FLUSH_INTERVAL_MS}ms")
    
    testEmulatorConnection()
  }

  override def invoke(event: EnrichedEvent): Unit = {
    synchronized {
      if (eventBuffer.size >= MAX_BUFFER_SIZE) {
        return
      }
      
      eventCount += 1
      eventBuffer += event
      
      if (eventCount <= 5 || eventCount % 100 == 0) {
        println(s"BigQuery buffered event #$eventCount: ${event.eventType} | ${event.contentType.getOrElse("N/A")}")
      }

      if (eventBuffer.size >= BATCH_SIZE) {
        flushBuffer(force = true)
      }
    }
  }

  private def testEmulatorConnection(): Unit = {
    try {
      val url = new URL(s"http://$emulatorHost:$emulatorPort")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(CONNECTION_TIMEOUT_MS)
      connection.setReadTimeout(READ_TIMEOUT_MS)
      
      val responseCode = connection.getResponseCode
      if (responseCode == 200) {
        println("✓ BigQuery emulator connection successful")
      } else {
        println(s"⚠ BigQuery emulator returned code: $responseCode")
      }
      connection.disconnect()
    } catch {
      case e: Exception =>
        println(s"⚠ BigQuery emulator connection failed: ${e.getMessage}")
        println("Will continue with file-based batching")
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
        def run(): Unit = flushToBigQuery(eventsToProcess)
      })
    }
  }

  private def flushToBigQuery(eventsToSend: List[EnrichedEvent]): Unit = {
    batchCount += 1
    
    try {
      val jsonPayload = createInsertPayload(eventsToSend)
      
      if (!sendHttpRequest(jsonPayload)) {
        writeToFile(eventsToSend)
      }
      
      println(s"BigQuery: Processed batch #$batchCount with ${eventsToSend.size} events")
      
    } catch {
      case e: Exception =>
        println(s"BigQuery batch #$batchCount error: ${e.getMessage}")
        writeToFile(eventsToSend)
    }
  }

  private def createInsertPayload(events: List[EnrichedEvent]): String = {
    val rows = events.map(convertEventToRow).mkString(",\n    ")
    s"""{
       |  "rows": [
       |    $rows
       |  ]
       |}""".stripMargin
  }

  private def convertEventToRow(event: EnrichedEvent): String = {
    val eventTimestamp = formatTimestamp(event.eventTs)
    val processingTimestamp = formatTimestamp(event.processingTime)
    
    s"""{
       |      "json": {
       |        "id": ${event.id},
       |        "content_id": "${event.contentId}",
       |        "user_id": "${event.userId}",
       |        "event_type": "${event.eventType}",
       |        "event_ts": "$eventTimestamp",
       |        "device": "${event.device}",
       |        "content_type": ${event.contentType.map(ct => s""""$ct"""").getOrElse("null")},
       |        "duration_ms": ${event.durationMs.getOrElse("null")},
       |        "engagement_pct": ${event.engagementPct.getOrElse("null")},
       |        "processing_time": "$processingTimestamp"
       |      }
       |    }""".stripMargin.replaceAll("\n", "")
  }

  private def formatTimestamp(timestamp: String): String = {
    try {
      val cleanTs = timestamp.replace("Z", "").replace("T", " ")
      if (cleanTs.contains(".")) {
        cleanTs.substring(0, cleanTs.indexOf("."))
      } else {
        cleanTs
      }
    } catch {
      case _: Exception => 
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    }
  }

  private def sendHttpRequest(jsonPayload: String): Boolean = {
    try {
      val url = new URL(s"http://$emulatorHost:$emulatorPort/projects/$projectId/datasets/$datasetId/tables/$tableId/insertAll")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setDoOutput(true)
      connection.setConnectTimeout(CONNECTION_TIMEOUT_MS)
      connection.setReadTimeout(READ_TIMEOUT_MS)
      
      val writer = new OutputStreamWriter(connection.getOutputStream, "UTF-8")
      writer.write(jsonPayload)
      writer.flush()
      writer.close()
      
      val responseCode = connection.getResponseCode
      connection.disconnect()
      
      if (responseCode == 200) {
        println(s"✓ BigQuery HTTP insert successful for batch #$batchCount")
        true
      } else {
        println(s"⚠ BigQuery HTTP insert failed with code: $responseCode")
        false
      }
    } catch {
      case e: Exception =>
        println(s"⚠ BigQuery HTTP error: ${e.getMessage}")
        false
    }
  }

  private def writeToFile(events: List[EnrichedEvent]): Unit = {
    try {
      val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
      val filename = s"/tmp/bigquery_batch_${timestamp}_${batchCount}.json"
      val writer = new BufferedWriter(new FileWriter(filename))

      events.foreach { event =>
        val jsonEvent = convertToJsonLine(event)
        writer.write(jsonEvent)
        writer.newLine()
      }

      writer.close()
      println(s"✓ BigQuery: Wrote ${events.size} events to $filename")
      
    } catch {
      case e: Exception =>
        println(s"✗ BigQuery file write error: ${e.getMessage}")
    }
  }

  private def convertToJsonLine(event: EnrichedEvent): String = {
    val eventTimestamp = formatTimestamp(event.eventTs)
    val processingTimestamp = formatTimestamp(event.processingTime)

    s"""{
      |  "id": ${event.id},
      |  "content_id": "${event.contentId}",
      |  "user_id": "${event.userId}",
      |  "event_type": "${event.eventType}",
      |  "event_ts": "$eventTimestamp",
      |  "device": "${event.device}",
      |  "content_type": ${event.contentType.map(ct => s""""$ct"""").getOrElse("null")},
      |  "duration_ms": ${event.durationMs.getOrElse("null")},
      |  "engagement_pct": ${event.engagementPct.getOrElse("null")},
      |  "processing_time": "$processingTimestamp"
      |}""".stripMargin.replaceAll("\n", "")
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
    
    println(s"BigQuery Sink closing - processed $eventCount events in $batchCount batches")
  }
}