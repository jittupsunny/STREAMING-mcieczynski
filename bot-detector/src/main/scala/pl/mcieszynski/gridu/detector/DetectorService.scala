package pl.mcieszynski.gridu.detector

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.spark.connector.writer._
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.util.{Either, Left, Right}


case class Event(timestamp: Long, categoryId: Int, ip: String, eventType: String)

case class EvaluatedEvent(baseEvent: Event, botDetected: Boolean)

case class InvalidEvent(originalMessage: String, exception: Throwable)

case class AggregateEventsCount(totalViews: Double, totalClicks: Int) {
  val viewRatio: Double = if (totalClicks > 0) totalViews / totalClicks else 0
}

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


    import spark.implicits._

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Seq(kafkaTopic), kafkaParams)

    /*val streamingContext = StreamingContext.getOrCreate(checkpointPath = checkpointDir, creatingFunc = () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
      val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
      kafkaStream.map(consumerRecord => tryEventConversion(consumerRecord.value()))
        .flatMap(_.right.toOption)
        .map(event => (event.ip, event))
        .mapValues(event => List(event))
        .reduceByKeyAndWindow((events, otherEvents) => events ++ otherEvents, (events, otherEvents) => events diff otherEvents,
          Seconds(10), Seconds(10))
        .mapWithState(
          StateSpec.function((_: String, newEventsOptional: Option[List[Event]],
                              aggData: State[AggregateEventsCount]) => {
            val newEvents = newEventsOptional.getOrElse(List.empty[Event])
            val calculatedAggTuple = newEvents.foldLeft((0.0, 0))((eventsAggregator, event) =>
              event.eventType match {
                case click => (eventsAggregator._1 + 1, eventsAggregator._2)
                case view => (eventsAggregator._1, eventsAggregator._2 + 1)
                case _ => (eventsAggregator._1, eventsAggregator._2)
              })
            val calculatedAgg = AggregateEventsCount(calculatedAggTuple._1, calculatedAggTuple._2)
            newEvents.map(event => EvaluatedEvent(event, Math.abs(calculatedAgg.totalClicks - calculatedAgg.totalViews) > 5))
          }))
        .stateSnapshots
        .print
      println("Finished setting up the context")
      ssc.checkpoint(directory = checkpointDir)
      ssc
    })*/

    val streamingContext = StreamingContext.getOrCreate(checkpointPath = checkpointDir, creatingFunc = () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
      val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
      kafkaStream.map(consumerRecord => tryEventConversion(consumerRecord.value()))
        .flatMap(_.right.toOption)
        .map(event => (event.ip, event))
        .mapValues(event => List(event))
        .reduceByKeyAndWindow((events, otherEvents) => events ++ otherEvents,
          (events, otherEvents) => events diff otherEvents, Seconds(4), Seconds(4))
        .print

      println("Finished setting up the context")
      ssc.checkpoint(checkpointDir)
      ssc
    })

    /*val streamingContext = StreamingContext.getActiveOrCreate(creatingFunc = () => {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
      val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
      kafkaStream.map(consumerRecord => tryEventConversion(consumerRecord.value()))
        .flatMap(_.right.toOption)
        .map(event => (event.ip, event))
        .mapValues(event => List(event))
        .reduceByKey((events, otherEvents) => events ++ otherEvents)
        .print

      println("Finished setting up the context")
      ssc
    })*/

    streamingContext.start
    streamingContext.awaitTermination
  }

  def tryEventConversion(jsonEvent: String): Either[InvalidEvent, Event] = {
    import net.liftweb.json._
    import scala.util.control.NonFatal
    try {
      val jsonMap = parse(jsonEvent).asInstanceOf[JObject].values
      val event = Event(jsonMap("unix_time").asInstanceOf[BigInt].toLong,
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
