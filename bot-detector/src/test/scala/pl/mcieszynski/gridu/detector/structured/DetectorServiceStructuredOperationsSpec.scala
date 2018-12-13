package pl.mcieszynski.gridu.detector.structured

import java.util.UUID

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.ignite.IgniteCache
import org.apache.ignite.spark.IgniteContext
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.DetectorServiceTestConstants
import pl.mcieszynski.gridu.detector.events.{AggregatedIpInformation, Event}

class DetectorServiceStructuredOperationsSpec extends WordSpec with DetectorServiceTestConstants with DatasetSuiteBase {

  lazy val sparkSession = DetectorServiceStructured.sparkSession

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
      val igniteContext = new IgniteContext(sparkSession.sparkContext, DetectorServiceStructured.igniteConfig)
      val igniteCache: IgniteCache[String, AggregatedIpInformation] = igniteContext.ignite().getOrCreateCache(DetectorServiceStructured.igniteDetectedBots)
      igniteCache.put(botIp, AggregatedIpInformation(botIp))

      val result = DetectorServiceStructured.filterKnownBotEvents(input, igniteContext.fromCache(DetectorServiceStructured.igniteDetectedBots)).collect()
      assert(1 == result.length)
      assert(validEvent == result(0))
    }
  }

}