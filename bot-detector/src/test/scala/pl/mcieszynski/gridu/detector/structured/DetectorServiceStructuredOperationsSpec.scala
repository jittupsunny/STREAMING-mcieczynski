package pl.mcieszynski.gridu.detector.structured

import java.util.UUID

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.sql.SparkSession
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.events.Event
import pl.mcieszynski.gridu.detector.DetectorServiceTestConstants

class DetectorServiceStructuredOperationsSpec extends WordSpec with DetectorServiceTestConstants with StreamingSuiteBase {
  val sparkSession: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("testing")
    .getOrCreate

  import sparkSession.implicits._

  "DetectorService" should {
    val validEvent = Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes).toString, timestamp, categoryId, ip, eventType)
    "convertValidEvents" in {
      val invalidRecord = ("1", "Invalid kafka stream entry")
      val validRecord = (kafkaMessageUUID, validEventJson)
      val input = List(invalidRecord, validRecord).toDS()

      val result = DetectorServiceStructured.retrieveEventsDataset(input).collect()
      assert(1 == result.length)
      assert(validEvent == result(0))
    }

    "filterKnownBotEvents" in {
      val input = List(validEvent,
        Event(UUID.nameUUIDFromBytes((1 + kafkaMessageUUID).getBytes).toString, timestamp, categoryId, botIp, eventType))
        .toDS()
      val output: List[List[(String, List[Event])]] = List(List((ip, List(validEvent))), List())

      val result = DetectorServiceStructured.filterKnownBotEvents(input, Array(botIp)).collect()
      assert(1 == result.length)
      assert(validEvent == result(0))
    }
  }

}