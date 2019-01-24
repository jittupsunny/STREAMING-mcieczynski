package pl.mcieszynski.gridu.detector

import java.util.UUID

import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import pl.mcieszynski.gridu.detector.events._

import scala.util.{Either, Left, Right}

trait DetectorService {

  var timeRatio = 1
  /**
    * all times in seconds
    */
  val TIME_WINDOW_LIMIT: Long = 600 / timeRatio

  val BATCH_DURATION: Long = 30 / timeRatio

  val SLIDE_DURATION: Long = 30 / timeRatio

  val kafkaTopic = "events"

  val kafkaGroup = "bot-detection"

  val bootstrapServers = "localhost:9092,localhost:9093,localhost:9094"

  val checkpointDir = "spark-checkpoint"

  val expiredEventsPredicate: BaseEvent => Boolean = event => event.timestamp > (System.currentTimeMillis() / 1000) - TIME_WINDOW_LIMIT

  val cassandraKeyspace = "bot_detection"

  val cassandraDetectedBots = "detected_bots"

  val cassandraEvents = "events"

  val igniteDetectedBots = "sharedRDD"

  def runService(args: Array[String])

  def simplifyEvents(events: List[Event]): List[SimpleEvent] = {
    events.map(event => simplifyEvent(event))
  }

  def simplifyEvent(event: Event): SimpleEvent = {
    SimpleEvent(event.uuid, event.timestamp, event.categoryId, event.eventType)
  }

  def tryEventConversion(kafkaMessageUUID: String, jsonEvent: String): Either[InvalidEvent, Event] = {
    import net.liftweb.json._

    import scala.util.control.NonFatal
    try {
      val jsonMap = parse(jsonEvent).asInstanceOf[JObject].values
      val event = Event(UUID.nameUUIDFromBytes(kafkaMessageUUID.getBytes()).toString,
        jsonMap("unix_time").asInstanceOf[BigInt].toLong,
        jsonMap("category_id").asInstanceOf[BigInt].toInt,
        jsonMap("ip").asInstanceOf[String],
        jsonMap("type").asInstanceOf[String])
      Right(event)
    } catch {
      case NonFatal(exception: Exception) => {
        println("Invalid event: ", jsonEvent, exception)
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
      //.enableHiveSupport
      .getOrCreate()
  }

  val igniteConfig = "pl/mcieszynski/gridu/ignite/ignite_configuration.xml"

  def igniteSetup(sparkSession: SparkSession, configPath: String = igniteConfig) = {
    new IgniteContext(sparkSession.sparkContext, configPath)
  }

  def kafkaSetup(): Map[String, Object]

  def kafkaMessageUUID(record: ConsumerRecord[String, String]): String = {
    record.topic() + "_" + record.partition() + "_" + record.offset()
  }

  def retrieveIgniteCache[K, V](igniteContext: IgniteContext, cacheName: String): IgniteRDD[K, V] = {
    igniteContext.fromCache(cacheName)
  }

}
