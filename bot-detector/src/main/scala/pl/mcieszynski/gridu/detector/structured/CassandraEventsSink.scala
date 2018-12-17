package pl.mcieszynski.gridu.detector.structured

import org.apache.spark.SparkConf
import pl.mcieszynski.gridu.detector.events.Event

class CassandraEventsSink(sparkConf: SparkConf) extends CassandraSink[Event](sparkConf) {

  override def process(event: Event): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
       insert into ${DetectorServiceStructured.cassandraKeyspace}.${DetectorServiceStructured.cassandraEvents} (uuid, category_id, event_type, ip, timestamp)
       values('${event.uuid}', ${event.categoryId}, '${event.eventType}', '${event.ip}', ${event.timestamp})""")
    })
  }

}
