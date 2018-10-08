package pl.mcieszynski.gridu.detector.events

import pl.mcieszynski.gridu.detector.DetectorServiceConstants._

case class AggregatedIpInformation(ip: String, currentEvents: List[SimpleEvent] = List.empty) {
  val botDetected: Option[String] = {
    if (currentEvents.size < BOT_DETECTION_THRESHOLD) {
      Option.empty // Too few requests
    } else if (currentEvents.size > REQUEST_LIMIT) {
      Option(REQUESTS) // Too many requests
    } else if (currentEvents.map(event => event.categoryId).distinct.count(_ => true) > CATEGORY_TYPE_LIMIT) {
      Option(CATEGORIES) // Too many categories
    } else {
      val countMap = currentEvents.groupBy(event => event.eventType).mapValues(eventList => eventList.size)
      val clickCount = countMap.getOrElse("click", 0).toDouble
      val viewCount = countMap.getOrElse("view", 0).toDouble
      if (clickCount > 0 && (viewCount == 0 || (clickCount / viewCount >= BOT_RATIO_LIMIT))) {
        Option(RATIO) // Too many clicks in comparison to the views
      } else Option.empty
    }
  }
}