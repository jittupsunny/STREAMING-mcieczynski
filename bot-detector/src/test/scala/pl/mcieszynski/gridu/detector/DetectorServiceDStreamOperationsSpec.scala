package pl.mcieszynski.gridu.detector

import java.util.UUID

import com.holdenkarau.spark.testing.StreamingSuiteBase
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.events.Event

class DetectorServiceDStreamOperationsSpec extends WordSpec with StreamingSuiteBase with EmbeddedKafka {

  val kafkaMessageUUID = "topic_partitionId_offsetId"

  val timestamp = 1538648621

  val categoryId = 1005

  val ip = "172.10.13.247"

  val eventType = "view"

  val validEventJson = "{\"unix_time\": " + 1538648621 + ", \"category_id\": " + categoryId +
    ", \"ip\": \"" + ip + "\", \"type\": \"" + eventType + "\"}"

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