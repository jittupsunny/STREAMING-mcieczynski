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

case class SimpleEvent(timestamp: Long, categoryId: Int, eventType: String)

case class AggregatedIpInformation(ip: String, currentEvents: List[SimpleEvent] = List.empty) {
  val botDetected: Boolean = currentEvents.size > REQUEST_LIMIT ||
    (currentEvents.map(event => event.categoryId).distinct.count(_ => true) >= CATEGORY_TYPE_LIMIT) || {
    val countMap = currentEvents.groupBy(event => event.eventType).mapValues(eventList => eventList.size)
    val clickCount = countMap.getOrElse("click", 0)
    val viewCount = countMap.getOrElse("view", 0)
    clickCount > 0 && (viewCount == 0 || clickCount / viewCount >= BOT_RATIO_LIMIT)
  }
}

//////////////////////

case class AggregatedEventsCount(clicks: Double, views: Double) {
  val clickViewRatio: Double = if (views > 0) clicks / views else BOT_RATIO_LIMIT
}

case class EvaluatedEvent(baseEvent: Event, isNormalUser: Boolean)


case class IpEvents(ip: String, clicks: Int, views: Int)

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
          StateSpec.function((_: String, newEventsOpt: Option[List[Event]], aggregatedEvents: State[AggregatedEventsCount]) => {
            val newEvents = newEventsOpt.getOrElse(List.empty[Event])
            val newAggregatedEvents = newEvents.foldLeft((0.0, 0.0))((eventsAggregator, event) =>
              event.eventType match {
                case "click" => (eventsAggregator._1 + 1, eventsAggregator._2)
                case "view" => (eventsAggregator._1, eventsAggregator._2 + 1)
                case _ => (eventsAggregator._1, eventsAggregator._2)
              })
            val calculatedAggregator = AggregatedEventsCount(newAggregatedEvents._1, newAggregatedEvents._2)
            aggregatedEvents.update(
              aggregatedEvents.getOption() match {
                case Some(aggregatedData) => AggregatedEventsCount(aggregatedData.clicks + calculatedAggregator.clicks,
                  aggregatedData.views + calculatedAggregator.views)
                case None => calculatedAggregator
              }
            )
            newEvents.map(event => EvaluatedEvent(event, aggregatedEvents.getOption()
              .map(_.clickViewRatio).getOrElse(BOT_RATIO_LIMIT) <= 5))
          })
        )
        .stateSnapshots()
        .map(ip_data => IpEvents(ip_data._1, ip_data._2.clicks.toInt, ip_data._2.views.toInt))
        .foreachRDD(rdd => {
          rdd.foreach(println)
          rdd.saveToCassandra("bot_detection", "ip_log", AllColumns)
        })

      println("Finished setting up the context")
      ssc.checkpoint(checkpointDir)
      ssc
    })

    streamingContext.start
    streamingContext.awaitTermination
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
