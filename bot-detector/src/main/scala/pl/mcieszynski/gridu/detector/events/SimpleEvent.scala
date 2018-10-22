package pl.mcieszynski.gridu.detector.events

import java.util.UUID

case class SimpleEvent(uuid: String, timestamp: Long, categoryId: Int, eventType: String) extends BaseEvent
