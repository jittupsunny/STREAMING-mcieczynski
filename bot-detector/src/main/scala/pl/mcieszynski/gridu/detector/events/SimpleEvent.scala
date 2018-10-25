package pl.mcieszynski.gridu.detector.events

case class SimpleEvent(uuid: String, timestamp: Long, categoryId: Int, eventType: String) extends BaseEvent
