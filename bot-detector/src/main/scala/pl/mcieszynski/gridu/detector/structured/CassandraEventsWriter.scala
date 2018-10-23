package pl.mcieszynski.gridu.detector.structured

import org.apache.spark.sql.ForeachWriter

class CassandraEventsWriter extends ForeachWriter[(String, String)] {

  override def open(partitionId: Long, version: Long): Boolean = ???

  override def process(value: (String, String)): Unit = ???

  override def close(errorOrNull: Throwable): Unit = ???

}
