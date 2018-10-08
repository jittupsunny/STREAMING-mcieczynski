package pl.mcieszynski.gridu.detector

import java.util.UUID

import pl.mcieszynski.gridu.detector.events.Event

trait DetectorServiceTestConstants {

  val click = "click"

  val view = "view"

  val kafkaMessageUUID = "topic_partitionId_offsetId"

  val timestamp = 1538648621

  val categoryId = 1005

  val ip = "172.10.13.247"

  val botIp = "172.20.13.247"

  val eventType = view

  val validEventJson = eventJson(timestamp, categoryId, ip, eventType)

  def eventJson(timestamp: Long = timestamp, categoryId: Int = categoryId, ip: String = ip, eventType: String = eventType): String = {
    "{\"unix_time\": " + timestamp + ", \"category_id\": " + categoryId +
      ", \"ip\": \"" + ip + "\", \"type\": \"" + eventType + "\"}"
  }

  def event(timestamp: Long = timestamp, categoryId: Int = categoryId, ip: String = ip, eventType: String = eventType, i: Int = 0): pl.mcieszynski.gridu.detector.events.Event = {
    Event(UUID.nameUUIDFromBytes((eventJson(timestamp, categoryId, ip, eventType) + i).getBytes), timestamp, categoryId, ip, eventType)
  }

}
