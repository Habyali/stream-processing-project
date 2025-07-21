package com.streaming.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

//=========
// Event Models

case class EngagementEvent(
  id: Long,
  contentId: String,
  userId: String,
  eventType: String,
  eventTs: String,
  durationMs: Option[Int],
  device: String,
  rawPayload: String
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class DebeziumPayload(
  id: Long,
  content_id: String,
  user_id: String,
  event_type: String,
  event_ts: String,
  duration_ms: Option[Int],
  device: String,
  raw_payload: String,
  __op: String,
  __table: String,
  __db: String,
  __ts_ms: Long
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class DebeziumMessage(
  payload: DebeziumPayload
)

case class ContentInfo(
  id: String,
  contentType: String,
  lengthSeconds: Option[Int]
)

case class EnrichedEvent(
  id: Long,
  contentId: String,
  userId: String,
  eventType: String,
  eventTs: String,
  device: String,
  contentType: Option[String],
  lengthSeconds: Option[Int],
  durationMs: Option[Int],
  engagementSeconds: Option[Double],
  engagementPct: Option[Double],
  processingTime: String
)