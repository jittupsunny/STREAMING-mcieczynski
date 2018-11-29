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

  val sparkSession = sparkSetup

  import sparkSession.implicits._

  def kafkaSetup() = {
    Map[String, String](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName,
      "group.id" -> kafkaGroup,
      "subscribe" -> kafkaTopic,
      "auto.offset.reset" -> "earliest",
      "rowsPerSecond" -> "4000",
      "spark.streaming.backpressure.enabled" -> (true: lang.Boolean).toString,
      "spark.streaming.backpressure.initialRate" -> "200000"
    )
  }

  def runService(args: Array[String]) {
    val igniteContext = igniteSetup(sparkSession)

    val kafkaParams = kafkaSetup()

    val dataFrame = sparkSession
      .readStream.format("kafka")
      .options(kafkaParams)
      .load()

    val compositeWriter = new CompositeWriter[(String, String)](Seq())

    val sharedRDD: IgniteRDD[String, AggregatedIpInformation] = retrieveIgniteCache(igniteContext, "sharedRDD")
    val previouslyDetectedBotIps = sharedRDD.keys.distinct.collect


    val eventsDataset = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .transform(retrieveEventsDataset)
    val eventsStream = eventsDataset
      .withWatermark("timestamp", "10 minute")
      .filter(event => !previouslyDetectedBotIps.contains(event.ip))

    storeEventsInCassandra(eventsStream)

    val query =
      eventsStream.writeStream
        .option("checkpointLocation", checkpointDir)
        .trigger(Trigger.ProcessingTime(SLIDE_DURATION + " second"))
        //.foreach(writer = compositeWriter)
        .start()

    val filteredEvents = filterKnownBotEvents(eventsStream, previouslyDetectedBotIps)

    val statefulIpDataset: Dataset[(String, AggregatedIpInformation)] = groupEventsByIpWithState(filteredEvents)

    val detectedBots = findNewBots(statefulIpDataset)

    storeNewBots(detectedBots, sharedRDD)
    query.awaitTermination
  }

  def retrieveEventsDataset(kafkaDataset: Dataset[(String, String)]): Dataset[Event] = {
    kafkaDataset
      .map(func = recordTuple => eventConversion(recordTuple._1, recordTuple._2))
      .filter(event => event != null)
  }

  def eventConversion(kafkaMessageUUID: String, jsonEvent: String): Event = {
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

  def storeEventsInCassandra(eventsMap: Dataset[Event]) = {
    eventsMap.rdd.saveToCassandra("bot_detection", "events",
      SomeColumns("uuid", "timestamp", "category_id", "ip", "event_type"))
  }

  def filterKnownBotEvents(eventsStream: Dataset[Event], previouslyDetectedBotIps: Array[String]): Dataset[Event] = {
    eventsStream
      .filter(event => !previouslyDetectedBotIps.contains(event.ip)) // Filter bot-confirmed events from further processing
  }


  def groupEventsByIpWithState(filteredEvents: Dataset[Event]): Dataset[(String, AggregatedIpInformation)] = {
    filteredEvents
      .groupByKey(event => event.ip)
      .mapGroupsWithState(func = stateMappingFunction)
  }

  def stateMappingFunction(ip: String, newEventsIterator: Iterator[Event], state: GroupState[AggregatedIpInformation]): (String, AggregatedIpInformation) = {
    val newEvents = newEventsIterator.toList.map(event => (event.uuid, event.timestamp, event.categoryId, event.eventType))
    val newState: AggregatedIpInformation = state.getOption match {
      case Some(aggregatedData) => {
        AggregatedIpInformation(ip, (aggregatedData.currentEvents ++ newEvents.map(fromEncoded)).distinct)
      }
      case None => AggregatedIpInformation(ip, newEvents.map(fromEncoded))
    }
    state.update(newState)
    (ip, toEncoded(newState))
  }

  def findNewBots(statefulIpDataset: Dataset[(String, AggregatedIpInformation)]): Dataset[(String, AggregatedIpInformation)] = {
    statefulIpDataset
      .filter(func = (aggregatedIpInformation: (String, AggregatedIpInformation)) => {
        aggregatedIpInformation._2.botDetected.nonEmpty
      })
  }

  def storeNewBots(detectedBots: Dataset[(String, AggregatedIpInformation)], sharedRDD: IgniteRDD[String, AggregatedIpInformation]) = {
    sharedRDD.savePairs(detectedBots.rdd.map(tuple => (tuple._1, fromEncoded(tuple._2))))
    detectedBots
      .map(aggregatedIpInformation =>
        DetectedBot(aggregatedIpInformation._1, System.currentTimeMillis(), aggregatedIpInformation._2.botDetected.get))
      .rdd.saveToCassandra("bot_detection", "detected_bots", AllColumns)
  }

}
