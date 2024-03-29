package pl.mcieszynski.gridu.detector

import org.apache.commons.cli._
import pl.mcieszynski.gridu.detector.dstream.{DetectorServiceDStream, DetectorServiceDStreamIgnite}
import pl.mcieszynski.gridu.detector.structured.DetectorServiceStructured

import scala.collection.immutable.ListMap

object MainDetectorService {


  val mode = "serviceMode"
  val modesMap = Map(
    "dstream" -> DetectorServiceDStream,
    "structured" -> DetectorServiceStructured,
    "ignite" -> DetectorServiceDStreamIgnite
  ).withDefaultValue(DetectorServiceDStream)

  val description = "Sets Streaming service to one of chosen modes: "
  val optionsMap = ListMap(
    mode -> new Option(mode, true, description + modesMap.keys.reduceLeft(_ + ", " + _))
  )

  def main(args: Array[String]): Unit = {
    val commandLine = parseCommandLineArgs(args)
    val chosenMode = commandLine.getOptionValue(mode)
    val detectorService = modesMap(chosenMode)
    detectorService.runService(args)
  }

  def parseCommandLineArgs(args: Array[String]): CommandLine = {
    val parser = new BasicParser()
    val options = new Options()
    optionsMap.foreach(option => options.addOption(option._2))

    parser.parse(options, args)
  }

}
