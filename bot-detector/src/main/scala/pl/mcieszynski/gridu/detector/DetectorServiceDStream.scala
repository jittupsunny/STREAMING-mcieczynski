package pl.mcieszynski.gridu.detector

import java.lang

import com.datastax.spark.connector._
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import pl.mcieszynski.gridu.detector.events.Event

object DetectorServiceDStream extends DetectorService {

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

  def main(args: Array[String]) {
    val sparkSession = sparkSetup
    val igniteContext = igniteSetup(sparkSession)

    val kafkaParams = kafkaSetup
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Seq(kafkaTopic), kafkaParams)


    val streamingContext = StreamingContext.getOrCreate(checkpointPath = checkpointDir, creatingFunc = () => {
      val (ssc, kafkaStream) = setupContextAndRetrieveDStream(sparkSession, consumerStrategy)
      val eventsMap = retrieveEventsDStream(kafkaStream)

      storeEventsInCassandra(eventsMap)

      val (sharedRDD: IgniteRDD[String, DetectorServiceDStream.AggregatedIpInformation], previouslyDetectedBotIps: Array[String]) = retrieveIgniteCache(igniteContext)

      val eventsWithinTheWindow = filterBotEventsInWindow(eventsMap, previouslyDetectedBotIps)

      val detectedBots = findBotsInTimeWindow(eventsWithinTheWindow)

      storeNewlyDetectedBots(detectedBots, sharedRDD)

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
      rdd.saveToCassandra("bot_detection", "events",
        SomeColumns("uuid", "timestamp", "category_id", "ip", "event_type")))
  }

  def retrieveIgniteCache(igniteContext: IgniteContext) = {
    val sharedRDD: IgniteRDD[String, AggregatedIpInformation] = igniteContext.fromCache("sharedRDD")
    val previouslyDetectedBotIps = sharedRDD.keys.distinct.collect
    (sharedRDD, previouslyDetectedBotIps)
  }

  def filterBotEventsInWindow(eventsMap: DStream[Event], previouslyDetectedBotIps: Array[String]): DStream[(String, List[Event])] = {
    eventsMap
      .filter(event => !previouslyDetectedBotIps.contains(event.ip)) // Filter bot-confirmed events from further processing
      .map(event => (event.ip, event))
      .mapValues(event => List(event))
      .reduceByKeyAndWindow((events, otherEvents) => (events ++ otherEvents).distinct,
        (events, otherEvents) => events,
        Seconds(TIME_WINDOW_LIMIT), Seconds(SLIDE_DURATION))
  }

  def findBotsInTimeWindow(windowReducedEvents: DStream[(String, List[Event])]): DStream[(String, AggregatedIpInformation)] = {
    windowReducedEvents.mapWithState(
      StateSpec.function((ip: String, newEventsOpt: Option[List[Event]], aggregatedIpInformation: State[AggregatedIpInformation]) => {
        val newEvents = newEventsOpt.getOrElse(List.empty[Event])
        val newAggregatedIpInformation = AggregatedIpInformation(ip, newEvents.map(simplifyEvent))
        aggregatedIpInformation.update(
          aggregatedIpInformation.getOption() match {
            case Some(aggregatedData) => {
              AggregatedIpInformation(ip, (aggregatedData.currentEvents ++ newAggregatedIpInformation.currentEvents).distinct)
            }
            case None => newAggregatedIpInformation
          }
        )
      })
    )
      .stateSnapshots()
      .filter(aggregatedIpInformation => aggregatedIpInformation._2.botDetected.nonEmpty)
  }

  def storeNewlyDetectedBots(detectedBots: DStream[(String, DetectorServiceDStream.AggregatedIpInformation)], sharedRDD: IgniteRDD[String, AggregatedIpInformation]) = {
    detectedBots.foreachRDD(detectedBotsRDD => sharedRDD.savePairs(detectedBotsRDD))

    detectedBots.map(aggregatedIpInformation => DetectedBot(aggregatedIpInformation._1, System.currentTimeMillis(), aggregatedIpInformation._2.botDetected.get))
      .foreachRDD(rdd => {
        rdd.foreach(println)
        rdd.saveToCassandra("bot_detection", "detected_bots", AllColumns)
      })
  }

}
