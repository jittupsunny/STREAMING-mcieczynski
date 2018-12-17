package pl.mcieszynski.gridu.detector.dstream

import java.lang

import com.datastax.spark.connector._
import org.apache.ignite.spark.IgniteRDD
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pl.mcieszynski.gridu.detector.DetectorService
import pl.mcieszynski.gridu.detector.events.{Event, IpInformation}

@Experimental
object DetectorServiceDStreamIgnite extends DetectorService {

  def kafkaSetup() = {
    Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroup,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
  }


  def runService(args: Array[String]) {
    val sparkSession = sparkSetup
    val igniteContext = igniteSetup(sparkSession)

    val kafkaParams = kafkaSetup()
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Seq(kafkaTopic), kafkaParams)

    val streamingContext = StreamingContext.getOrCreate(checkpointPath = checkpointDir, creatingFunc = () => {
      val (ssc, kafkaStream) = setupContextAndRetrieveDStream(sparkSession, consumerStrategy)
      val eventsMap = retrieveEventsDStream(kafkaStream)

      storeEventsInCassandra(eventsMap)

      val botsRDD: IgniteRDD[String, IpInformation] = retrieveIgniteCache(igniteContext, "botsRDD")
      val eventsRDD: IgniteRDD[String, Event] = retrieveIgniteCache(igniteContext, "eventsRDD")

      val previouslyDetectedBotIps = botsRDD.keys.distinct.collect

      val filteredEvents = filterKnownBotEvents(eventsMap, previouslyDetectedBotIps)

      val timestampWindow = System.currentTimeMillis() / 1000 - TIME_WINDOW_LIMIT
      val inMemoryEvents = eventsRDD
        .map(pair => pair._2)
        .filter(event => event.timestamp > timestampWindow)
        .map(event => (event.ip, event))
        .mapValues(event => List(event))

      filteredEvents.foreachRDD(rdd => {
        val mergedRDD: RDD[(String, List[Event])] = joinCachedEvents(inMemoryEvents, rdd)
        val (newBotsRDD: RDD[(String, IpInformation)], otherEventsRDD: RDD[(String, Event)]) = splitBotEvents(mergedRDD)
        botsRDD.savePairs(newBotsRDD)
        eventsRDD.savePairs(otherEventsRDD)
      })

      println("Finished setting up the context")
      ssc.checkpoint(checkpointDir)
      ssc
    })

    streamingContext.start
    streamingContext.awaitTermination
  }

  def setupContextAndRetrieveDStream(sparkSession: SparkSession, consumerStrategy: ConsumerStrategy[String, String]) = {
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(BATCH_DURATION))
    val kafkaStream = KafkaUtils
      .createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
      .map(record => (kafkaMessageUUID(record), record.value()))
    (ssc, kafkaStream)
  }


  def retrieveEventsDStream(kafkaStream: DStream[(String, String)]) = {
    kafkaStream
      .map(recordTuple => tryEventConversion(recordTuple._1, recordTuple._2))
      .flatMap(_.right.toOption)
  }

  def storeEventsInCassandra(eventsMap: DStream[Event]) = {
    eventsMap.foreachRDD(rdd =>
      rdd.saveToCassandra("bot_detection", "events",
        SomeColumns("uuid", "timestamp", "category_id", "ip", "event_type")))
  }

  def filterKnownBotEvents(eventsStream: DStream[Event], previouslyDetectedBotIps: Array[String]): DStream[(String, List[Event])] = {
    eventsStream
      .filter(event => !previouslyDetectedBotIps.contains(event.ip)) // Filter bot-confirmed events from further processing
      .map(event => (event.ip, event))
      .mapValues(event => List(event))
  }


  def joinCachedEvents(inMemoryEvents: RDD[(String, List[Event])], rdd: RDD[(String, List[Event])]) = {
    val keys = rdd.keys.collect()
    val mergedRDD = rdd.leftOuterJoin(inMemoryEvents.filter(pair => keys.contains(pair._1)))
      .map(joinedEvents => {
        val ip = joinedEvents._1
        val events = joinedEvents._2._1
        val cachedEvents = joinedEvents._2._2.getOrElse(List.empty)
        (ip, events ++ cachedEvents)
      })
    mergedRDD
  }

  def splitBotEvents(mergedRDD: RDD[(String, List[Event])]) = {
    val ipInformationRDD = mergedRDD
      .map(ipEvents => IpInformation(ipEvents._1, ipEvents._2))

    val newBotsRDD = ipInformationRDD
      .filter(ipInformation => ipInformation.botDetected.nonEmpty)
      .map(ipInformation => (ipInformation.ip, ipInformation))

    val otherEventsRDD = ipInformationRDD
      .filter(ipInformation => ipInformation.botDetected.isEmpty)
      .flatMap(ipInformation => ipInformation.currentEvents)
      .map(event => (event.uuid, event))
    (newBotsRDD, otherEventsRDD)
  }


}
