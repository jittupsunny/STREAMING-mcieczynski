package pl.mcieszynski.gridu.detector.service

import org.scalatest.WordSpec
import pl.mcieszynski.gridu.detector.MainDetectorService
import pl.mcieszynski.gridu.detector.dstream.DetectorServiceDStream
import pl.mcieszynski.gridu.detector.structured.DetectorServiceStructured

class MainDetectorServiceSpec extends WordSpec {

  "MainDetectorService" should {
    "parse structured mode" in {
      val args: Array[String] = Array("-serviceMode", "structured")
      val commandLine = MainDetectorService.parseCommandLineArgs(args)
      val mode = commandLine.getOptionValue(MainDetectorService.mode)

      assert(DetectorServiceStructured == MainDetectorService.modesMap(mode))
    }

    "parse as default mode" in {
      val args: Array[String] = Array("-serviceMode", "none")
      val commandLine = MainDetectorService.parseCommandLineArgs(args)
      val mode = commandLine.getOptionValue(MainDetectorService.mode)

      assert(DetectorServiceDStream == MainDetectorService.modesMap(mode))
    }

    "use default mode" in {
      val args: Array[String] = Array()
      val commandLine = MainDetectorService.parseCommandLineArgs(args)
      val mode = commandLine.getOptionValue(MainDetectorService.mode)

      assert(DetectorServiceDStream == MainDetectorService.modesMap(mode))
    }
  }

}
