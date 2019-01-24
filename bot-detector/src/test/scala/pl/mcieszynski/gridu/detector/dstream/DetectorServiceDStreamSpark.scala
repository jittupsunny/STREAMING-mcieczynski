package pl.mcieszynski.gridu.detector.dstream

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterEach, WordSpec}
import pl.mcieszynski.gridu.detector.DetectorServiceTestConstants
import pl.mcieszynski.gridu.detector.events.{AggregatedIpInformation, Event}

import scala.collection.mutable


class DetectorServiceDStreamSpark extends WordSpec with DetectorServiceTestConstants with BeforeAndAfterEach {

  var sparkSession: SparkSession = _

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  "BasicExample" should {
    "print" in {
      val sparkContext: SparkContext = sparkSession.sparkContext
      val data: Seq[(String, String)] = Seq(("a", "1"), ("b", "2"), ("c", "3"))
      val rdd: RDD[(String, String)] = sparkContext.parallelize(data)
      val strings: mutable.Queue[RDD[(String, String)]] = mutable.Queue.empty[RDD[(String, String)]]
      val streamingContext = new StreamingContext(sparkContext, Seconds(1))
      val dStream: InputDStream[(String, String)] = streamingContext.queueStream(strings)
      strings += rdd
      dStream.groupByKeyAndWindow(Seconds(1)).print
      streamingContext.start
      streamingContext.awaitTerminationOrTimeout(5 * 1000)
    }
  }

  "DetectorServiceDStreamSpark" should {
    "filterNewBots" in {
      val timeWindow = 10
      lazy val sparkContext = sparkSession.sparkContext
      val userList = List((ip, user(ip)))
      val bot2Ip = "172.20.14.247"
      val botsList = List((botIp, categoriesBot(botIp)), (bot2Ip, ratioBot(bot2Ip)))

      val input: List[(String, List[Event])] = userList ++ botsList

      val rdd = sparkContext.parallelize(input)
      val queue: mutable.Queue[RDD[(String, List[Event])]] = mutable.Queue.empty[RDD[(String, List[Event])]]
      val checkpointPath = DetectorServiceDStream.checkpointDir + "/DetectorServiceDStreamSpark/filterNewBots"
      queue += rdd

      val streamingContext = StreamingContext.getActiveOrCreate(checkpointPath = checkpointPath, creatingFunc = () => {
        lazy val streamingContext = new StreamingContext(sparkContext, Seconds(timeWindow))
        streamingContext.checkpoint(checkpointPath)
        val inputDstream = streamingContext.queueStream(queue)
        val windowed = inputDstream.window(Seconds(timeWindow))

        val resultingBots: DStream[(String, AggregatedIpInformation)] = DetectorServiceDStream.findNewBotsInWindow(windowed)
        resultingBots.foreachRDD(rdd => rdd.foreach(tuple => {
          println(tuple._2)
          if (tuple._2.botDetected.isEmpty) throw new Exception("Test failed - a non-bot passed through the filtering")
        }))


        streamingContext
      })
      streamingContext.start
      streamingContext.awaitTerminationOrTimeout(timeWindow * 2 * 1000)

      FileUtils.deleteDirectory(new File(checkpointPath))
    }
  }

  override def afterEach() {
    sparkSession.stop()
  }

}
