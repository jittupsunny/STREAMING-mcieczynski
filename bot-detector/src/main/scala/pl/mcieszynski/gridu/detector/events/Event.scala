package pl.mcieszynski.gridu.detector.events

case class Event(uuid: String, timestamp: Long, categoryId: Int, ip: String, eventType: String) extends BaseEvent
