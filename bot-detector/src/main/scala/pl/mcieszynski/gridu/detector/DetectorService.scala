package pl.mcieszynski.gridu.detector

import java.util.UUID

import com.datastax.spark.connector._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import pl.mcieszynski.gridu.detector.DetectorConstants._

import scala.util.{Either, Left, Right}

case class Event(uuid: UUID, timestamp: Long, categoryId: Int, ip: String, eventType: String)

case class InvalidEvent(originalMessage: String, exception: Throwable)

case class SimpleEvent(uuid: UUID, timestamp: Long, categoryId: Int, eventType: String)

case class AggregatedIpInformation(ip: String, currentEvents: List[SimpleEvent] = List.empty) {
  val botDetected: Option[String] = if (currentEvents.size > REQUEST_LIMIT) Option(REQUESTS)
  else if (currentEvents.map(event => event.categoryId).distinct.count(_ => true) >= CATEGORY_TYPE_LIMIT) Option(CATEGORIES)
  else {
    val countMap = currentEvents.groupBy(event => event.eventType).mapValues(eventList => eventList.size)
    val clickCount = countMap.getOrElse("click", 0)
    val viewCount = countMap.getOrElse("view", 0)
    if (clickCount > 0 && viewCount > 0 && (clickCount / viewCount >= BOT_RATIO_LIMIT)) Option(RATIO) else Option.empty
  }
}

case class DetectedBot(ip: String, timestamp: Long, reason: String)

object DetectorService {

  val kafkaTopic = "events"

  val kafkaGroup = "bot-detection"

  val checkpointDir = "file:////Users/mcieszynski/prg/data/spark/bot-detection-checkpoint"

  val bootstrapServers = "localhost:9092,localhost:9093,localhost:9094"

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("Bot Detector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport
      .getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroup,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Seq(kafkaTopic), kafkaParams)

    val streamingContext = StreamingContext.getOrCreate(checkpointPath = checkpointDir, creatingFunc = () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(BATCH_DURATION))
      val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
      val flatMap = kafkaStream.map(consumerRecord => tryEventConversion(consumerRecord.value()))
        .flatMap(_.right.toOption)
      flatMap.foreachRDD(rdd =>
        rdd.saveToCassandra("bot_detection", "events",
          SomeColumns("uuid", "timestamp", "category_id", "ip", "event_type")))
      flatMap.map(event => (event.ip, event))
        .mapValues(event => List(event))
        .reduceByKeyAndWindow((events, otherEvents) => events ++ otherEvents,
          (events, otherEvents) => events diff otherEvents,
          Seconds(TIME_WINDOW_LIMIT), Seconds(SLIDE_DURATION))
        .mapWithState(
          StateSpec.function((ip: String, newEventsOpt: Option[List[Event]], aggregatedIpInformation: State[AggregatedIpInformation]) => {
            val newEvents = newEventsOpt.getOrElse(List.empty[Event])
            val newAggregatedIpInformation = AggregatedIpInformation(ip, newEvents.map(simplifyEvent))

            aggregatedIpInformation.update(
              aggregatedIpInformation.getOption() match {
                case Some(aggregatedData) => {
                  val validEvents = aggregatedData.currentEvents.filter(event => event.timestamp < (System.currentTimeMillis() / 1000))
                  AggregatedIpInformation(ip, (validEvents ++ newAggregatedIpInformation.currentEvents).distinct)
                }
                case None => newAggregatedIpInformation
              }
            )
          })
        )
        .stateSnapshots()
        .filter(aggregatedIpInformation => aggregatedIpInformation._2.botDetected.nonEmpty)
        .map(aggregatedIpInformation => DetectedBot(aggregatedIpInformation._1, System.currentTimeMillis(), aggregatedIpInformation._2.botDetected.get))
        .foreachRDD(rdd => {
          rdd.foreach(println)
          rdd.saveToCassandra("bot_detection", "detected_bots", AllColumns)
        })

      println("Finished setting up the context")
      ssc.checkpoint(checkpointDir)
      ssc
    })

    streamingContext.start
    streamingContext.awaitTermination
  }

  def simplifyEvent(event: Event): SimpleEvent = {
    SimpleEvent(event.uuid, event.timestamp, event.categoryId, event.eventType)
  }

  def tryEventConversion(jsonEvent: String): Either[InvalidEvent, Event] = {
    import net.liftweb.json._

    import scala.util.control.NonFatal
    try {
      val jsonMap = parse(jsonEvent).asInstanceOf[JObject].values
      val event = Event(UUID.nameUUIDFromBytes(jsonEvent.getBytes()),
        jsonMap("unix_time").asInstanceOf[BigInt].toLong,
        jsonMap("category_id").asInstanceOf[BigInt].toInt,
        jsonMap("ip").asInstanceOf[String],
        jsonMap("type").asInstanceOf[String])
      Right(event)
    } catch {
      case NonFatal(exception: Exception) => {
        println("Invalid event:", exception)
        Left(InvalidEvent(jsonEvent, exception))
      }
    }
  }
}
