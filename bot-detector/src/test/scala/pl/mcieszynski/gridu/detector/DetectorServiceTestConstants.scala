package pl.mcieszynski.gridu.detector

import java.util.UUID

import pl.mcieszynski.gridu.detector.DetectorServiceConstants.{BOT_DETECTION_THRESHOLD, BOT_RATIO_LIMIT, REQUEST_LIMIT}
import pl.mcieszynski.gridu.detector.events.Event

trait DetectorServiceTestConstants {

  val click = "click"

  val view = "view"

  val kafkaMessageUUID = "topic_partitionId_offsetId"

  val timestamp = System.currentTimeMillis

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
    Event(UUID.nameUUIDFromBytes((eventJson(timestamp, categoryId, ip, eventType) + i).getBytes).toString, timestamp, categoryId, ip, eventType)
  }

  def categoriesBot(botIp: String): List[Event] = {
    List.range(0, BOT_DETECTION_THRESHOLD + 1)
      .map(i => event((timestamp + i), categoryId + i, botIp, eventType = if (i % 2 == 0) click else view))
  }

  def requestsBot(botIp: String): List[Event] = {
    List.range(0, REQUEST_LIMIT + 1)
      .map(i => event(timestamp + i % 4 + 1, categoryId + i % 2, botIp, eventType = if (i % 2 == 0) click else view, i))
  }

  def ratioBot(botIp: String): List[Event] = {
    List.range(0, BOT_DETECTION_THRESHOLD + 1)
      .map(i => event(timestamp + i, categoryId + i % 2, botIp, eventType = if (i % (BOT_RATIO_LIMIT * 2) > 0) click else view))
  }

  def noViewsBot(botIp: String): List[Event] = {
    List.range(0, BOT_DETECTION_THRESHOLD + 1)
      .map(i => event(timestamp + i, categoryId + i % 2, botIp, eventType = click))
  }

  def user(userIp: String): List[Event] = {
    List.range(0, BOT_DETECTION_THRESHOLD + 1)
      .map(i => event(timestamp + i, categoryId + i % 2, userIp, eventType = if (i % 2 == 0) click else view))
  }


}
