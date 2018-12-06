package pl.mcieszynski.gridu.detector.structured

import java.lang
import java.util.UUID

import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.spark.IgniteRDD
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{GroupState, OutputMode, Trigger}
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

    val sharedRDD: IgniteRDD[String, AggregatedIpInformation] = retrieveIgniteCache(igniteContext, igniteDetectedBots)
    val previouslyDetectedBotIps = sharedRDD.keys.distinct.collect


    val eventsDataset = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .transform(retrieveEventsDataset)
    val allEventsStream = eventsDataset
      .withWatermark("timestamp", "10 minute")

    val cassandraEventsQuery = allEventsStream.writeStream
      .trigger(Trigger.ProcessingTime(SLIDE_DURATION + " second"))
      .option("checkpointLocation", checkpointDir + "/cassandra/events")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", cassandraKeyspace)
      .option("table", cassandraEvents)
      .outputMode(OutputMode.Update)
      .start()

    val filteredEvents = filterKnownBotEvents(allEventsStream, previouslyDetectedBotIps)
    val statefulIpDataset: Dataset[(String, AggregatedIpInformation)] = groupEventsByIpWithState(filteredEvents)
    val detectedBots = findNewBots(statefulIpDataset)

    val cassandraBotsQuery = detectedBots
      .map(aggregatedIpInformation =>
        DetectedBot(aggregatedIpInformation._1, System.currentTimeMillis(), aggregatedIpInformation._2.botDetected.get))
      .writeStream
      .trigger(Trigger.ProcessingTime(SLIDE_DURATION + " second"))
      .option("checkpointLocation", checkpointDir + "/cassandra/detedtedBots")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", cassandraKeyspace)
      .option("table", cassandraDetectedBots)
      .outputMode(OutputMode.Update)
      .start()

    val igniteBotsQuery = detectedBots
      .map(tuple => (tuple._1, fromEncoded(tuple._2)))
      .writeStream
      .trigger(Trigger.ProcessingTime(SLIDE_DURATION + " second"))
      .option("checkpointLocation", checkpointDir + "/ignite/detedtedBots")
      .format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, igniteConfig)
      .option(OPTION_TABLE, igniteDetectedBots)
      .outputMode(OutputMode.Update)
      .start()

    sparkSession.streams.awaitAnyTermination()
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
}
