package pl.mcieszynski.gridu.detector.events

import java.util.UUID

case class Event(uuid: UUID, timestamp: Long, categoryId: Int, ip: String, eventType: String) extends BaseEvent
