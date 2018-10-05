package pl.mcieszynski.gridu.detector.events

import java.util.UUID

case class SimpleEvent(uuid: UUID, timestamp: Long, categoryId: Int, eventType: String) extends BaseEvent
