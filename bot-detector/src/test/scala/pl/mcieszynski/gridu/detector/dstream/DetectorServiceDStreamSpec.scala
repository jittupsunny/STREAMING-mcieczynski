package pl.mcieszynski.gridu.detector.dstream

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import pl.mcieszynski.gridu.detector.dstream


class DetectorServiceDStreamSpec extends WordSpec with BeforeAndAfterAll with EmbeddedKafka with MockitoSugar {

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  val kafkaParams = DetectorServiceDStream.kafkaSetup()
  val topic = dstream.DetectorServiceDStream.kafkaTopic

  val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](Seq(topic), kafkaParams)
  try {
    EmbeddedKafka.start()(config)
    lazy val sparkSession: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("testing")
      .getOrCreate
    val streamingService = DetectorServiceDStream
    "DetectorService" ignore  {
      "setup context and create DStream" in {
        withRunningKafka {
          val (ssc, dStream) = streamingService.setupContextAndRetrieveDStream(sparkSession, consumerStrategy)
          assert(dStream.slideDuration == Seconds(DetectorServiceDStream.BATCH_DURATION))
          assert(ssc != null)
        }
      }
    }
  }
  finally {
    EmbeddedKafka.stop()
  }

}