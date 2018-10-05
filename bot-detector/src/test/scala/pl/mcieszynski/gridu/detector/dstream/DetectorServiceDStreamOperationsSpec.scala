package pl.mcieszynski.gridu.detector.dstream

import java.util.UUID

import com.holdenkarau.spark.testing.StreamingSuiteBase
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.{DetectorServiceDStream, DetectorServiceTestConstants}
import pl.mcieszynski.gridu.detector.events.Event

class DetectorServiceDStreamOperationsSpec extends WordSpec with DetectorServiceTestConstants with StreamingSuiteBase with EmbeddedKafka {

  "DetectorService" should {
    "convertValidEvents" in {
      val invalidRecord = ("1", "Invalid kafka stream entry")
      val validRecord = (kafkaMessageUUID, validEventJson)
      val input = List(List(invalidRecord, validRecord))
      val output = List(List(Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes), timestamp, categoryId, ip, eventType)))
      testOperation(input, DetectorServiceDStream.retrieveEventsDStream, output, ordered = true)
    }
  }
}