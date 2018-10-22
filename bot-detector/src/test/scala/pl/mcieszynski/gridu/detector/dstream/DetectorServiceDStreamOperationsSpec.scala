package pl.mcieszynski.gridu.detector.dstream

import java.util.UUID

import com.holdenkarau.spark.testing.StreamingSuiteBase
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.events.Event
import pl.mcieszynski.gridu.detector.{DetectorServiceDStream, DetectorServiceTestConstants}

class DetectorServiceDStreamOperationsSpec extends WordSpec with DetectorServiceTestConstants with StreamingSuiteBase with EmbeddedKafka {

  "DetectorService" should {
    val validEvent = Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes).toString, timestamp, categoryId, ip, eventType)
    "convertValidEvents" in {
      val invalidRecord = ("1", "Invalid kafka stream entry")
      val validRecord = (kafkaMessageUUID, validEventJson)
      val input = List(List(invalidRecord, validRecord))
      val output = List(List(validEvent))
      testOperation(input, DetectorServiceDStream.retrieveEventsDStream, output, ordered = true)
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
  }

}