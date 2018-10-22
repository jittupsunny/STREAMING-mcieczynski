package pl.mcieszynski.gridu.detector

import org.apache.commons.lang3.StringUtils
import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.DetectorServiceConstants._
import pl.mcieszynski.gridu.detector.events.AggregatedIpInformation

class DetectorServiceAggregationSpec extends WordSpec with DetectorServiceTestConstants {

  "EventsAggregator" should {
    "recognizeTooManyCategories" in {
      val input = List.range(0, BOT_DETECTION_THRESHOLD + 1)
        .map(i => event((timestamp + i).toLong, categoryId + i, ip, eventType = if (i % 2 == 0) click else view))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == CATEGORIES)
    }

    "recognizeTooManyRequests" in {
      val input = List.range(0, REQUEST_LIMIT + 1)
        .map(i => event((timestamp + i % 4 + 1).toLong, categoryId + i % 2, ip, eventType = if (i % 2 == 0) click else view, i))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == REQUESTS)
    }

    "recognizeTooHighClickRatio" in {
      val input = List.range(0, BOT_DETECTION_THRESHOLD + 1)
        .map(i => event((timestamp + i).toLong, categoryId + i % 2, ip, eventType = if (i % (BOT_RATIO_LIMIT * 2) > 0) click else view))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == RATIO)
    }

    "recognizeNoViews" in {
      val input = List.range(0, BOT_DETECTION_THRESHOLD + 1)
        .map(i => event((timestamp + i).toLong, categoryId + i % 2, ip, eventType = click))
      val output = AggregatedIpInformation(ip, input.map(DetectorServiceDStream.simplifyEvent))

      assert(output.botDetected.getOrElse(StringUtils.EMPTY) == RATIO)
    }
  }


}