package pl.mcieszynski.gridu.detector

import org.apache.commons.lang3.StringUtils
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.DetectorServiceConstants._
import pl.mcieszynski.gridu.detector.dstream.DetectorServiceDStream
import pl.mcieszynski.gridu.detector.events.AggregatedIpInformation

class DetectorServiceAggregationSpec extends WordSpec with DetectorServiceTestConstants {

  "EventsAggregator" should {
    "recognizeTooManyCategories" in {
      val input = categoriesBot(botIp)
      val output = AggregatedIpInformation(botIp, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == CATEGORIES)
    }

    "recognizeTooManyRequests" in {
      val input = requestsBot(botIp)
      val output = AggregatedIpInformation(botIp, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == REQUESTS)
    }

    "recognizeTooHighClickRatio" in {
      val input = ratioBot(botIp)
      val output = AggregatedIpInformation(botIp, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == RATIO)
    }

    "recognizeNoViews" in {
      val input = List.range(0, BOT_DETECTION_THRESHOLD + 1)
        .map(i => event((timestamp + i).toLong, categoryId + i % 2, ip, eventType = click))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == RATIO)
    }

    "splitUserFromBots" in {
      val userIp = ip;
      val user1 = user(userIp)

      val bot1Ip = "127.20.13.1"
      val bot2Ip = "127.20.13.2"
      val bot1 = ratioBot(bot1Ip)
      val bot2 = categoriesBot(bot2Ip)

      val eventsByIpList = List(AggregatedIpInformation(userIp, user1.map(DetectorServiceDStream.simplifyEvent)),
        AggregatedIpInformation(bot1Ip, bot1.map(DetectorServiceDStream.simplifyEvent)),
        AggregatedIpInformation(bot2Ip, bot2.map(DetectorServiceDStream.simplifyEvent)))

      assert(2 == eventsByIpList.count(ipInformation => ipInformation.botDetected.nonEmpty))
    }
  }

}