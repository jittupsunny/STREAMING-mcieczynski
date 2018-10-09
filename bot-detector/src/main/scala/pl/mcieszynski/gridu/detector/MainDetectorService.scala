package pl.mcieszynski.gridu.detector

object MainDetectorService {

  def main(args: Array[String]): Unit = {
    val detectorService = DetectorServiceDStream
    detectorService.runService(args)
  }

}
