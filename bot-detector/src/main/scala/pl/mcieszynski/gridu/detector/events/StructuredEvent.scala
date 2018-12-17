package pl.mcieszynski.gridu.detector.events

import java.sql.Timestamp

case class StructuredEvent(uuid: String, timestamp: Long, categoryId: Int, ip: String, eventType: String, structuredTimestamp: Timestamp) extends BaseEvent
