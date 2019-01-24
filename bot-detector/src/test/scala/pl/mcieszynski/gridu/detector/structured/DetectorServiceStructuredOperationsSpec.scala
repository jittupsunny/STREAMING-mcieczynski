package pl.mcieszynski.gridu.detector.structured

import java.sql.Timestamp
import java.util.UUID

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.ignite.IgniteCache
import org.apache.ignite.spark.IgniteContext
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.DetectorServiceTestConstants
import pl.mcieszynski.gridu.detector.events.{AggregatedIpInformation, Event, StructuredEvent}

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

    "filterKnownBotEvents" ignore {
      val structuredEvent = StructuredEvent(validEvent.uuid, validEvent.timestamp, validEvent.categoryId, validEvent.ip, validEvent.eventType, new Timestamp(validEvent.timestamp))
      val input = List(structuredEvent,
        StructuredEvent(UUID.nameUUIDFromBytes((1 + kafkaMessageUUID).getBytes).toString, timestamp, categoryId, botIp, eventType, new Timestamp(timestamp)))
        .toDS()
      val output: List[List[(String, List[Event])]] = List(List((ip, List(validEvent))), List())
      val igniteContext = new IgniteContext(sparkSession.sparkContext, DetectorServiceStructured.igniteConfig)
      val igniteCache: IgniteCache[String, AggregatedIpInformation] = igniteContext.ignite().getOrCreateCache(DetectorServiceStructured.igniteDetectedBots)
      igniteCache.put(botIp, AggregatedIpInformation(botIp))

      val result = DetectorServiceStructured.filterKnownBotEvents(input, igniteContext.fromCache(DetectorServiceStructured.igniteDetectedBots)).collect()
      assert(1 == result.length)
      assert(structuredEvent == result(0))
    }

    "groupEventsByIp" in {
      val botEvents = ratioBot(botIp)
      val userEvents = user(ip)
      val expectedMap: Map[String, List[String]] = Map((botIp, botEvents.map(event => event.uuid)), (ip, userEvents.map(event => event.uuid)))


      val input = (userEvents ++ botEvents).map(DetectorServiceStructured.convertToStructuredEvent)
      val inputDS = input.toDS()

      val result = DetectorServiceStructured.groupEventsByIpWithState(inputDS)
      val tuples = result.take(10)
      assert(expectedMap.size == tuples.length)
      tuples.foreach(ipInformation => {
        assert(expectedMap.contains(ipInformation._1))
        val eventsUuids = expectedMap(ipInformation._1)
        val ipUuids = ipInformation._2.currentEvents.map(event => event.uuid)
        assert(ipUuids.forall(eventsUuids.contains))
        assert(eventsUuids.forall(ipUuids.contains))
        //        ipUuids.foreach(uuid => {
        //          println(if (eventsUuids.contains(uuid)) "OK: " else "MISSING: ", uuid)
        //        })
      })
    }

    "findNewBots" in {
      val botEvents = ratioBot(botIp)
      val userEvents = user(ip)
      val expectedMap: Map[String, List[String]] = Map((botIp, botEvents.map(event => event.uuid)), (ip, userEvents.map(event => event.uuid)))


      val input = (userEvents ++ botEvents).map(DetectorServiceStructured.convertToStructuredEvent)
      val inputDS = input.toDS()
      val groupedEvents = DetectorServiceStructured.groupEventsByIpWithState(inputDS)

      val result = DetectorServiceStructured.findNewBots(groupedEvents)

      val tuple = result.take(10)
      assert(1 == tuple.length)
      assert(tuple(0)._2.botDetected.nonEmpty)
    }
  }
}