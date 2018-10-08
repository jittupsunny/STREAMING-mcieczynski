package pl.mcieszynski.gridu.detector.dstream

import java.util.UUID

import com.holdenkarau.spark.testing.StreamingSuiteBase
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.DetectorServiceConstants._
import pl.mcieszynski.gridu.detector.events.{AggregatedIpInformation, Event}
import pl.mcieszynski.gridu.detector.{DetectorServiceDStream, DetectorServiceTestConstants}

class DetectorServiceDStreamOperationsSpec extends WordSpec with DetectorServiceTestConstants with StreamingSuiteBase with EmbeddedKafka {

  "DetectorService" should {
    val validEvent = Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes), timestamp, categoryId, ip, eventType)
    "convertValidEvents" in {
      val invalidRecord = ("1", "Invalid kafka stream entry")
      val validRecord = (kafkaMessageUUID, validEventJson)
      val input = List(List(invalidRecord, validRecord))
      val output = List(List(validEvent))
      testOperation(input, DetectorServiceDStream.retrieveEventsDStream, output, ordered = true)
    }

    "filterKnownBotEvents" in {
      val input: List[List[Event]] = List(List(validEvent),
        List(Event(UUID.nameUUIDFromBytes((1 + kafkaMessageUUID).getBytes), timestamp, categoryId, botIp, eventType)))
      val output: List[List[(String, List[Event])]] = List(List((ip, List(validEvent))), List())

      def internalMethod(stream: DStream[Event]): DStream[(String, List[Event])] = {
        DetectorServiceDStream.filterKnownBotEvents(stream, Array(botIp))
      }

      testOperation(input, internalMethod, output, ordered = true)
    }
  }

  "EventsAggregator" should {
    "recognizeTooManyCategories" in {
      val input = List.range(0, BOT_DETECTION_THRESHOLD + 1)
        .map(i => event((timestamp + i).toLong, categoryId + i, ip, eventType = if (i % 2 == 0) click else view))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == CATEGORIES)
    }

    "recognizeTooManyRequests" in {
      val input = List.range(0, REQUEST_LIMIT + 1)
        .map(i => event((timestamp + i % 4 + 1).toLong, categoryId + i % 2, ip, eventType = if (i % 2 == 0) click else view, i))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == REQUESTS)
    }

    "recognizeTooHighClickRatio" in {
      val input = List.range(0, BOT_DETECTION_THRESHOLD + 1)
        .map(i => event((timestamp + i).toLong, categoryId + i % 2, ip, eventType = if (i % (BOT_RATIO_LIMIT * 2) > 0) click else view))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == RATIO)
    }

    "recognizeNoViews" in {
      val input = List.range(0, BOT_DETECTION_THRESHOLD + 1)
        .map(i => event((timestamp + i).toLong, categoryId + i % 2, ip, eventType = click))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == RATIO)
    }
  }


}