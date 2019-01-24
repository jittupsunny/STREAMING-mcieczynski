package pl.mcieszynski.gridu.detector.dstream

import java.util.UUID

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.DetectorServiceTestConstants
import pl.mcieszynski.gridu.detector.events.{AggregatedIpInformation, Event}


class DetectorServiceDStreamOperationsSpec extends WordSpec with DetectorServiceTestConstants with StreamingSuiteBase {

  "DetectorService" should {
    val validEvent = Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes).toString, timestamp, categoryId, ip, eventType)
    "convertValidEvents" in {
      val invalidRecord = ("1", "Invalid kafka stream entry")
      val validRecord = (kafkaMessageUUID, validEventJson)
      val input = List(List(invalidRecord, validRecord))
      val output = List(List(validEvent))
      testOperation(input, DetectorServiceDStream.retrieveEventsDStream, output, ordered = true)
    }

    "reduceEventsInWindow" in {
      val userEvents = user(ip)
      val input: List[List[(String, List[Event])]] = List(userEvents.map(event => (event.ip, List(event))))
      val output: List[List[(String, List[Event])]] = List(List((ip, userEvents)))

      testOperation(input, reduceEventsInWindow, output, ordered = false)
    }

    "filterKnownBotEvents" in {
      val input: List[List[Event]] = List(List(validEvent),
        List(Event(UUID.nameUUIDFromBytes((1 + kafkaMessageUUID).getBytes).toString, timestamp, categoryId, botIp, eventType)))
      val output: List[List[(String, List[Event])]] = List(List((ip, List(validEvent))), List())

      def internalMethod(stream: DStream[Event]): DStream[(String, List[Event])] = {
        DetectorServiceDStream.filterKnownBotEvents(stream, Array(botIp))
      }

      testOperation(input, internalMethod, output, ordered = true)
    }

    "filterNewBots" in {
      val userList = List((ip, user(ip)))
      val bot2Ip = "172.20.14.247"
      val bot1Events = categoriesBot(botIp)
      val bot2Events = ratioBot(bot2Ip)
      val botsList = List((botIp, bot1Events), (bot2Ip, bot2Events))
      val input: List[List[(String, List[Event])]] = List(userList ++ botsList)

      val output: List[List[(String, AggregatedIpInformation)]] = List(List((botIp, AggregatedIpInformation(botIp, bot1Events.map(DetectorServiceDStream.simplifyEvent))),
        (bot2Ip, AggregatedIpInformation(bot2Ip, bot2Events.map(DetectorServiceDStream.simplifyEvent)))))

      testOperation(input, DetectorServiceDStream.findNewBotsInWindow, output, ordered = false)
    }
  }

  /**
    * [[pl.mcieszynski.gridu.detector.dstream.DetectorServiceDStream#reduceEventsInWindow(org.apache.spark.streaming.dstream.DStream]]
    * This method mocks the reduceEventsInWindow from DetectorService using smaller slice duration to match the test streaming context's parameters
    */
  def reduceEventsInWindow(eventsMap: DStream[(String, List[Event])]): DStream[(String, List[Event])] = {
    eventsMap.reduceByKeyAndWindow((events, otherEvents) => (events ++ otherEvents).distinct,
      (events, otherEvents) => events.filter(event => !otherEvents.contains(event)),
      Seconds(1), Seconds(1))
  }

  def reduce(events: List[Event], otherEvents: List[Event]): List[Event] = {
    (events ++ otherEvents).distinct
  }
}