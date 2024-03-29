package pl.mcieszynski.gridu.detector.dstream

import java.lang

import com.datastax.spark.connector._
import org.apache.ignite.spark.IgniteRDD
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import pl.mcieszynski.gridu.detector.DetectorService
import pl.mcieszynski.gridu.detector.events.{AggregatedIpInformation, DetectedBot, Event}

object DetectorServiceDStream extends DetectorService {

  val sparkSession = sparkSetup

  def kafkaSetup() = {
    Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroup,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean),
      "spark.streaming.backpressure.enabled" -> (false: lang.Boolean),
      "spark.streaming.backpressure.initialRate" -> (200000: lang.Integer)
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

      val sharedRDD: IgniteRDD[String, AggregatedIpInformation] = retrieveIgniteCache(igniteContext, igniteDetectedBots)
      val previouslyDetectedBotIps = sharedRDD.keys.distinct.collect

      val filteredEvents = filterKnownBotEvents(eventsMap, previouslyDetectedBotIps)

      val eventsWithinTheWindow = reduceEventsInWindow(filteredEvents)

      val detectedBots = findNewBotsInWindow(eventsWithinTheWindow)

      storeNewBots(detectedBots, sharedRDD)

      println("Finished setting up the context")
      ssc.checkpoint(checkpointDir)
      ssc
    })

    streamingContext.start
    streamingContext.awaitTermination
  }

  def setupContextAndRetrieveDStream(sparkSession: SparkSession, consumerStrategy: ConsumerStrategy[String, String]) = {
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(BATCH_DURATION))
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
      .map(record => (kafkaMessageUUID(record), record.value()))
    (ssc, kafkaStream)
  }


  def retrieveEventsDStream(kafkaStream: DStream[(String, String)]) = {
    kafkaStream.map(recordTuple => tryEventConversion(recordTuple._1, recordTuple._2))
      .flatMap(_.right.toOption)
  }

  def storeEventsInCassandra(eventsMap: DStream[Event]) = {
    eventsMap.foreachRDD(rdd =>
      rdd.saveToCassandra(cassandraKeyspace, cassandraEvents,
        SomeColumns("uuid", "timestamp", "category_id", "ip", "event_type")))
  }

  def filterKnownBotEvents(eventsStream: DStream[Event], previouslyDetectedBotIps: Array[String]): DStream[(String, List[Event])] = {
    eventsStream
      .filter(event => !previouslyDetectedBotIps.contains(event.ip)) // Filter bot-confirmed events from further processing
      .map(event => (event.ip, event))
      .mapValues(event => List(event))
  }

  def reduceEventsInWindow(eventsMap: DStream[(String, List[Event])]): DStream[(String, List[Event])] = {
    eventsMap.reduceByKeyAndWindow((events, otherEvents) => (events ++ otherEvents).distinct,
      (events, otherEvents) => events.filter(event => !otherEvents.contains(event)),
      Seconds(TIME_WINDOW_LIMIT), Seconds(SLIDE_DURATION))
  }

  def resolveIpEventsState = {
    (ip: String, newEventsOpt: Option[List[Event]], aggregatedIpInformation: State[AggregatedIpInformation]) => {
      val newEvents = newEventsOpt.getOrElse(List.empty[Event])
      val newAggregatedIpInformation = AggregatedIpInformation(ip, newEvents.map(simplifyEvent))
      val validTimeLimit = System.currentTimeMillis() - TIME_WINDOW_LIMIT * 1000;
      aggregatedIpInformation.update(
        aggregatedIpInformation.getOption() match {
          case Some(aggregatedData) => {
            val information = AggregatedIpInformation(ip, (aggregatedData.currentEvents.filter(event => event.timestamp > validTimeLimit) ++ newAggregatedIpInformation.currentEvents).distinct)
            //            println("OLD", aggregatedData.currentEvents)
            //            println("FILTERED", aggregatedData.currentEvents.filter(event => event.timestamp > validTimeLimit))
            //            println("NEW", newAggregatedIpInformation.currentEvents)
            information
          }
          case None => newAggregatedIpInformation
        }
      )
    }
  }

  def findNewBotsInWindow(windowReducedEvents: DStream[(String, List[Event])]): DStream[(String, AggregatedIpInformation)] = {
    windowReducedEvents.mapWithState(StateSpec.function(resolveIpEventsState))
      .stateSnapshots()
      .filter(aggregatedIpInformation => aggregatedIpInformation._2.botDetected.nonEmpty)
  }

  def storeNewBots(detectedBots: DStream[(String, AggregatedIpInformation)], sharedRDD: IgniteRDD[String, AggregatedIpInformation]) = {
    detectedBots.foreachRDD(detectedBotsRDD => sharedRDD.savePairs(detectedBotsRDD))

    detectedBots.map(aggregatedIpInformation => DetectedBot(aggregatedIpInformation._1, System.currentTimeMillis(), aggregatedIpInformation._2.botDetected.get))
      .foreachRDD(rdd => {
        rdd.foreach(println)
        rdd.saveToCassandra(cassandraKeyspace, cassandraDetectedBots, AllColumns)
      })
  }

}
