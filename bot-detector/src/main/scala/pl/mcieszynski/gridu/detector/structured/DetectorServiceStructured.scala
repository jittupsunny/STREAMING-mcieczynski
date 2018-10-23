package pl.mcieszynski.gridu.detector.structured

import java.lang
import java.util.UUID

import com.datastax.spark.connector._
import org.apache.ignite.spark.IgniteRDD
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{GroupState, Trigger}
import pl.mcieszynski.gridu.detector.DetectorService
import pl.mcieszynski.gridu.detector.events.{AggregatedIpInformation, DetectedBot, Event}

object DetectorServiceStructured extends DetectorService with EventsEncoding {

  def kafkaSetup() = {
    Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroup,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean),
      "spark.streaming.backpressure.enabled" -> (true: lang.Boolean),
      "spark.streaming.backpressure.initialRate" -> (200000: lang.Integer)
    )
  }

  def runService(args: Array[String]) {
    val sparkSession = sparkSetup
    val igniteContext = igniteSetup(sparkSession)

    import sparkSession.implicits._

    val kafkaParams = kafkaSetup()

    val dataFrame = sparkSession
      .readStream.format("kafka")
      .option("bootstrap.servers", bootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("group.id", kafkaGroup)
      .option("rowsPerSecond", 4000)
      .option("encoding", "UTF-8")
      .load()

    val compositeWriter = new CompositeWriter[(String, String)](Seq())

    val query = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .transform(retrieveEventsDataset)
      .writeStream
      .option("encoding", "UTF-8")
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime(SLIDE_DURATION + " seconds"))
      //.foreach(writer = compositeWriter)
      .start()

    /*val eventsMap = Map()

    storeEventsInCassandra(eventsMap)

    val sharedRDD: IgniteRDD[String, AggregatedIpInformation] = retrieveIgniteCache(igniteContext, "sharedRDD")

    val previouslyDetectedBotIps = sharedRDD.keys.distinct.collect

    val filteredEvents = filterKnownBotEvents(eventsMap, previouslyDetectedBotIps)

    val statefulIpDataset: Dataset[(String, AggregatedIpInformationEncoded)] = groupEventsByIpWithState(filteredEvents)

    val detectedBots = findNewBots(statefulIpDataset)

    storeNewBots(detectedBots, sharedRDD)*/
    query.awaitTermination
  }


  def retrieveEventsDataset(kafkaDataset: Dataset[(String, String)]): Dataset[EventEncoded] = {
    kafkaDataset
      .map(func = recordTuple => toEncoded(eventConversion(recordTuple._1, recordTuple._2)))
      .filter(event => event != null)
  }

  def eventConversion(kafkaMessageUUID: String, jsonEvent: String): EventEncoded = {
    import net.liftweb.json._

    import scala.util.control.NonFatal
    try {
      val jsonMap = parse(jsonEvent).asInstanceOf[JObject].values
      val event = Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes()).toString,
        jsonMap("unix_time").asInstanceOf[BigInt].toLong,
        jsonMap("category_id").asInstanceOf[BigInt].toInt,
        jsonMap("ip").asInstanceOf[String],
        jsonMap("type").asInstanceOf[String])
      event
    } catch {
      case NonFatal(exception: Exception) => {
        println("Invalid event:", jsonEvent, exception)
        null
      }
    }
  }

  def storeEventsInCassandra(eventsMap: Dataset[EventEncoded]) = {
    eventsMap.rdd.saveToCassandra("bot_detection", "events",
      SomeColumns("uuid", "timestamp", "category_id", "ip", "event_type"))
  }

  def filterKnownBotEvents(eventsStream: Dataset[EventEncoded], previouslyDetectedBotIps: Array[String]): Dataset[EventEncoded] = {
    eventsStream
      .filter(event => !previouslyDetectedBotIps.contains(event.ip)) // Filter bot-confirmed events from further processing
  }

  def groupEventsByIpWithState(filteredEvents: Dataset[EventEncoded]): Dataset[(String, AggregatedIpInformationEncoded)] = {
    filteredEvents
      .groupByKey(event => event.ip)
      .mapGroupsWithState(func = stateMappingFunction)
  }

  def stateMappingFunction(ip: String, newEventsIterator: Iterator[EventEncoded], state: GroupState[AggregatedIpInformation]): (String, AggregatedIpInformationEncoded) = {
    val newEvents = newEventsIterator.toList.map(event => (event.uuid, event.timestamp, event.categoryId, event.eventType))
    val newState: AggregatedIpInformation = state.getOption match {
      case Some(aggregatedData) => {
        val validTimestamp = System.currentTimeMillis() - TIME_WINDOW_LIMIT
        val noOutdatedEvents = aggregatedData.currentEvents.filter(event => event.timestamp > validTimestamp)
        AggregatedIpInformation(ip, (noOutdatedEvents ++ newEvents.map(fromEncoded)).distinct)
      }
      case None => AggregatedIpInformation(ip, newEvents.map(fromEncoded))
    }
    state.update(newState)
    (ip, toEncoded(newState))
  }

  def findNewBots(statefulIpDataset: Dataset[(String, AggregatedIpInformationEncoded)]): Dataset[(String, AggregatedIpInformationEncoded)] = {
    statefulIpDataset
      .filter(func = (aggregatedIpInformation: (String, AggregatedIpInformationEncoded)) => {
        aggregatedIpInformation._2.botDetected.nonEmpty
      })
  }

  def storeNewBots(detectedBots: Dataset[(String, AggregatedIpInformationEncoded)], sharedRDD: IgniteRDD[String, AggregatedIpInformation]) = {
    sharedRDD.savePairs(detectedBots.rdd.map(tuple => (tuple._1, fromEncoded(tuple._2))))
    detectedBots
      .map(aggregatedIpInformation =>
        DetectedBot(aggregatedIpInformation._1, System.currentTimeMillis(), aggregatedIpInformation._2.botDetected.get))
      .rdd.saveToCassandra("bot_detection", "detected_bots", AllColumns)
  }

}
