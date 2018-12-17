package pl.mcieszynski.gridu.detector.structured

import org.apache.spark.SparkConf
import pl.mcieszynski.gridu.detector.events.DetectedBot

class CassandraBotsSink(sparkConf: SparkConf) extends CassandraSink[DetectedBot](sparkConf) {

  override def process(detectedBot: DetectedBot): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
       insert into ${DetectorServiceStructured.cassandraKeyspace}.${DetectorServiceStructured.cassandraDetectedBots} (ip, reason, timestamp)
       values('${detectedBot.ip}', '${detectedBot.reason}', ${detectedBot.timestamp})""")
    })
  }

}
