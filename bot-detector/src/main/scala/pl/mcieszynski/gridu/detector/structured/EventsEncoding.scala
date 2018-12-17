package pl.mcieszynski.gridu.detector.structured

import java.sql.Timestamp

import pl.mcieszynski.gridu.detector.events._

trait EventsEncoding {

  type EventEncoded = (String, Long, Int, String, String)

  implicit def toEncoded(event: Event): EventEncoded = (event.uuid, event.timestamp, event.categoryId, event.ip, event.eventType)

  implicit def fromEncoded(encoded: EventEncoded): Event = Event(encoded._1, encoded._2, encoded._3, encoded._4, encoded._5)

  type SimpleEventEncoded = (String, Long, Int, String)

  implicit def toEncoded(simpleEvent: SimpleEvent): SimpleEventEncoded = (simpleEvent.uuid, simpleEvent.timestamp, simpleEvent.categoryId, simpleEvent.eventType)

  implicit def fromEncoded(encoded: SimpleEventEncoded): SimpleEvent = SimpleEvent(encoded._1, encoded._2, encoded._3, encoded._4)

  type AggregatedIpInformationEncoded = (String, Seq[SimpleEventEncoded])

  implicit def toEncoded(aggregatedIpInformation: AggregatedIpInformation): AggregatedIpInformationEncoded
  = (aggregatedIpInformation.ip, aggregatedIpInformation.currentEvents.map(simpleEvent => toEncoded(simpleEvent)))

  implicit def fromEncoded(encoded: AggregatedIpInformationEncoded): AggregatedIpInformation
  = AggregatedIpInformation(encoded._1, encoded._2.map(fromEncoded).toList)

  type DetectedBotEncoded = (String, Long, String)

  implicit def toEncoded(detectedBot: DetectedBot): DetectedBotEncoded = (detectedBot.ip, detectedBot.timestamp, detectedBot.reason)

  implicit def fromEncoded(encoded: DetectedBotEncoded): DetectedBot = DetectedBot(encoded._1, encoded._2, encoded._3)

  type StructuredEventEncoded = (String, Long, Int, String, String, Long)

  implicit def toEncoded(structuredEvent: StructuredEvent): StructuredEventEncoded = (structuredEvent.uuid, structuredEvent.timestamp, structuredEvent.categoryId, structuredEvent.ip, structuredEvent.eventType, structuredEvent.structuredTimestamp.getTime)

  implicit def fromEncoded(encoded: StructuredEventEncoded): StructuredEvent = StructuredEvent(encoded._1, encoded._2, encoded._3, encoded._4, encoded._5, new Timestamp(encoded._6))

}
