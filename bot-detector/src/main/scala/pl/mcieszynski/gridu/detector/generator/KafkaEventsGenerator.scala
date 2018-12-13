package pl.mcieszynski.gridu.detector.generator

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.parsing.json.JSONObject

object KafkaEventsGenerator {

  val userCategories = List.fill(4)(Range(1000, 1005).toList).flatten
  val botCategories = Range(1000, 1020).toList

  val userActions = List("click", "view", "view", "view")
  val botActions = List("view", "click", "click", "click")

  val BOT_TRANSITION_EVERY_SEC = 2

  def randomContentUser() = userCategories(Random.nextInt(userCategories.length))

  def randomContentBot() = botCategories(Random.nextInt(botCategories.length))

  def randomActionUser() = userActions(Random.nextInt(userActions.length))

  def randomActionBot() = botActions(Random.nextInt(botActions.length))

  def user2ip(id: Int) = "172.10.%d.%d".format(id / 255, id % 255)

  def bot2ip(id: Int) = "172.20.%d.%d".format(id / 255, id % 255)

  def asits(dt: DateTime) = dt.toDate.getTime

  def message(ip: String, eventTime: Long, eventType: String, categoryId: Int) = {
    val message = JSONObject(Map("ip" -> ip, "unix_time" -> eventTime, "category_id" -> categoryId, "type" -> eventTime))
    message.toString()
  }

  def kafkaSetup() = {
    Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    )
  }

  val kafkaProperties = new Properties()
  kafkaProperties.putAll(kafkaSetup().asJava)
  val producer = new KafkaProducer[String, String](kafkaProperties)

  protected val running = true

  def shutdown(): Unit = {
    if (running) {
      producer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    sys.addShutdownHook(shutdown())
    val Array(bots, users, duration, freq) = args
    generateEvents(bots.toInt, users.toInt, duration.toInt, freq.toInt)
  }

  protected val eventsTopic = "events"

  protected val NEXT_SECOND = 1

  def generateEvents(botsNumber: Int, usersNumber: Int, duration: Int, freq: Int): Unit = {
    var dateTime = DateTime.now().withMillisOfSecond(0)
    var startMilis = dateTime.toDate.getTime
    val users = Range(0, usersNumber).toList
    val bots = Range(0, botsNumber).toList

    for (i: Int <- 1 to duration) {
      for (f: Int <- 1 to freq) {
        val user = users(Random.nextInt(usersNumber))
        val payload = message(user2ip(user), dateTime.minusSeconds(Random.nextInt(60)).toDate.getTime, randomActionUser(), randomContentUser())
        producer.send(new ProducerRecord[String, String](eventsTopic, payload))
      }
      if (i % BOT_TRANSITION_EVERY_SEC == 0) {
        bots.foreach(bot => {
          val payload = message(bot2ip(bot), dateTime.minusSeconds(Random.nextInt(60)).toDate.getTime, randomActionBot(), randomContentBot())
          producer.send(new ProducerRecord[String, String](eventsTopic, payload))
        })
      }
      dateTime = dateTime.plusSeconds(NEXT_SECOND)
      while (dateTime.isAfter(DateTime.now())) {
        Thread.sleep(10)
      }
    }
    shutdown()
  }

}
