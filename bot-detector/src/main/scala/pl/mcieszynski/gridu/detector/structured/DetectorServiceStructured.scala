package pl.mcieszynski.gridu.detector.structured

import java.lang
import java.sql.Timestamp
import java.util.UUID

import org.apache.ignite.spark.IgniteRDD
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Dataset
import pl.mcieszynski.gridu.detector.DetectorService
import pl.mcieszynski.gridu.detector.events._

object DetectorServiceStructured extends DetectorService with EventsEncoding {

  val sparkSession = sparkSetup

  import sparkSession.implicits._

  def kafkaSetup() = {
    Map[String, String](
      "kafka.bootstrap.servers" -> bootstrapServers,
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
    //val igniteContext = igniteSetup(sparkSession)

    val kafkaParams = kafkaSetup()

    val dataFrame = sparkSession
      .readStream.format("kafka")
      .options(kafkaParams)
      .load()

    //val sharedRDD: IgniteRDD[String, AggregatedIpInformation] = retrieveIgniteCache(igniteContext, igniteDetectedBots)

    val eventsDataset = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .transform(retrieveEventsDataset)

    val cassandraEventsQuery = eventsDataset.writeStream
      //.trigger(Trigger.ProcessingTime(SLIDE_DURATION + " second"))
      .option("checkpointLocation", checkpointDir + "/cassandra/events")
      .foreach(new CassandraEventsSink(sparkSession.sparkContext.getConf))
      .start()

    val allEventsStream = eventsDataset
      .map(convertToStructuredEvent)
      .withWatermark("structuredTimestamp", "10 minute")

    //val filteredEvents = filterKnownBotEvents(allEventsStream, sharedRDD)
    val groupedEvents: Dataset[(String, AggregatedIpInformation)] = groupEventsByIpWithState(allEventsStream)
    val detectedBots = findNewBots(groupedEvents)

    /*val igniteBotsQuery = detectedBots
      .map(tuple => (tuple._1, fromEncoded(tuple._2)))
      .writeStream
      .trigger(Trigger.ProcessingTime(SLIDE_DURATION + " second"))
      .option("checkpointLocation", checkpointDir + "/ignite/detedtedBots")
      .foreach(new IgniteSink())
      .outputMode(OutputMode.Update)
      .start()*/

    val cassandraBotsQuery = detectedBots
      .map(aggregatedIpInformation =>
        DetectedBot(aggregatedIpInformation._1, System.currentTimeMillis(), aggregatedIpInformation._2.botDetected.get))
      .writeStream
      //.trigger(Trigger.ProcessingTime(SLIDE_DURATION + " second"))
      .option("checkpointLocation", checkpointDir + "/cassandra/detedtedBots")
      .foreach(new CassandraBotsSink(sparkSession.sparkContext.getConf))
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

  def filterKnownBotEvents(eventsStream: Dataset[StructuredEvent], sharedRDD: IgniteRDD[String, AggregatedIpInformation]): Dataset[StructuredEvent] = {
    val previouslyDetectedBotIps = sharedRDD.keys.distinct.collect
    eventsStream
      .filter(event => !previouslyDetectedBotIps.contains(event.ip)) // Filter bot-confirmed events from further processing
  }

  def groupEventsByIpWithState(filteredEvents: Dataset[StructuredEvent]): Dataset[(String, AggregatedIpInformation)] = {
    filteredEvents
      .groupByKey(event => event.ip)
      .mapGroups((ip, ipEvents) => (ip, AggregatedIpInformation(ip, ipEvents.map(simplifyStructuredEvent).toList)))
  }

  def findNewBots(statefulIpDataset: Dataset[(String, AggregatedIpInformation)]): Dataset[(String, AggregatedIpInformation)] = {
    statefulIpDataset
      .filter(func = (aggregatedIpInformation: (String, AggregatedIpInformation)) => {
        aggregatedIpInformation._2.botDetected.nonEmpty
      })
  }

  def simplifyStructuredEvents(events: List[StructuredEvent]): List[SimpleEvent] = {
    events.map(event => simplifyStructuredEvent(event))
  }

  def simplifyStructuredEvent(event: StructuredEvent): SimpleEvent = {
    SimpleEvent(event.uuid, event.timestamp, event.categoryId, event.eventType)
  }

  def convertToStructuredEvent: Event => StructuredEvent = {
    event => StructuredEvent(event.uuid, event.timestamp, event.categoryId, event.ip, event.eventType, new Timestamp(event.timestamp))
  }

}
