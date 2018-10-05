package pl.mcieszynski.gridu.detector.dstream

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import pl.mcieszynski.gridu.detector.DetectorServiceDStream

class DetectorServiceDStreamSpec extends WordSpec with BeforeAndAfterAll with EmbeddedKafka with MockitoSugar {
  lazy val sparkSession: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("testing")
    .getOrCreate
  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)


  val streamingService = DetectorServiceDStream
  var ssc: StreamingContext = _
  var dStream: DStream[(String, String)] = _

  val kafkaParams = DetectorServiceDStream.kafkaSetup()
  val topic = pl.mcieszynski.gridu.detector.DetectorServiceDStream.kafkaTopic

  val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](Seq(topic), kafkaParams)
  try {
    EmbeddedKafka.start()(config)
    "DetectorService" should {
      "create DStream" in {
        withRunningKafka {
          val (tmpSsc, tmpDStream) = streamingService.setupContextAndRetrieveDStream(sparkSession, consumerStrategy)
          ssc = tmpSsc
          dStream = tmpDStream
          assert(dStream.slideDuration == Seconds(DetectorServiceDStream.BATCH_DURATION))
        }
      }

    }
  }
  finally {
    EmbeddedKafka.stop()
  }

}