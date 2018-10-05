package pl.mcieszynski.gridu.detector

import java.util.UUID

import org.apache.ignite.spark.IgniteContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import pl.mcieszynski.gridu.detector.events.{BaseEvent, Event, InvalidEvent, SimpleEvent}

import scala.util.{Either, Left, Right}

trait DetectorService {

  val timeRatio = 1

  val BOT_RATIO_LIMIT = 5

  val REQUEST_LIMIT: Int = 1000 / timeRatio

  val CATEGORY_TYPE_LIMIT = 5

  /**
    * all times in seconds
    */
  val TIME_WINDOW_LIMIT: Long = 600 / timeRatio

  val BATCH_DURATION: Long = 60 / timeRatio

  val SLIDE_DURATION: Long = 600 / timeRatio

  val REQUESTS = "requests"

  val CATEGORIES = "categories"

  val RATIO = "ratio"

  val kafkaTopic = "events"

  val kafkaGroup = "bot-detection"

  val bootstrapServers = "localhost:9092,localhost:9093,localhost:9094"

  val checkpointDir = "file:////Users/mcieszynski/prg/data/spark/bot-detection-checkpoint"

  val expiredEventsPredicate: BaseEvent => Boolean = event => event.timestamp > (System.currentTimeMillis() / 1000) - TIME_WINDOW_LIMIT

  case class AggregatedIpInformation(ip: String, currentEvents: List[SimpleEvent] = List.empty) {
    val botDetected: Option[String] = if (currentEvents.size > REQUEST_LIMIT) Option(REQUESTS)
    else if (currentEvents.map(event => event.categoryId).distinct.count(_ => true) > CATEGORY_TYPE_LIMIT) Option(CATEGORIES)
    else {
      val countMap = currentEvents.groupBy(event => event.eventType).mapValues(eventList => eventList.size)
      val clickCount = countMap.getOrElse("click", 0)
      val viewCount = countMap.getOrElse("view", 0)
      if (clickCount > 0 && viewCount > 0 && (clickCount / viewCount >= BOT_RATIO_LIMIT)) Option(RATIO) else Option.empty
    }
  }

  case class DetectedBot(ip: String, timestamp: Long, reason: String)


  def simplifyEvent(event: Event): SimpleEvent = {
    SimpleEvent(event.uuid, event.timestamp, event.categoryId, event.eventType)
  }

  def tryEventConversion(kafkaMessageUUID: String, jsonEvent: String): Either[InvalidEvent, Event] = {
    import net.liftweb.json._

    import scala.util.control.NonFatal
    try {
      val jsonMap = parse(jsonEvent).asInstanceOf[JObject].values
      val event = Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes()),
        jsonMap("unix_time").asInstanceOf[BigInt].toLong,
        jsonMap("category_id").asInstanceOf[BigInt].toInt,
        jsonMap("ip").asInstanceOf[String],
        jsonMap("type").asInstanceOf[String])
      Right(event)
    } catch {
      case NonFatal(exception: Exception) => {
        println("Invalid event:", jsonEvent, exception)
        Left(InvalidEvent(jsonEvent, exception))
      }
    }
  }

  def sparkSetup = {
    SparkSession.builder
      .master("local[3]")
      .appName("Bot Detector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport
      .getOrCreate()
  }


  def igniteSetup(sparkSession: SparkSession, configPath: String = "file:////Users/mcieszynski/prg/code/final-project/ignite_configuration.xml") = {
    new IgniteContext(sparkSession.sparkContext, configPath)
  }


  def kafkaSetup(): Map[String, Object]

  def kafkaMessageUUID(record: ConsumerRecord[String, String]): String = {
    record.topic() + "_" + record.partition() + "_" + record.offset()
  }
}
