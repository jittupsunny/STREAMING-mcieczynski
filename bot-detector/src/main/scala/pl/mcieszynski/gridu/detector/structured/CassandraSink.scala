package pl.mcieszynski.gridu.detector.structured

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.ForeachWriter
import pl.mcieszynski.gridu.detector.events.Event

abstract class CassandraSink[T](sparkConf: SparkConf) extends ForeachWriter[T] {

  var cassandraConnector: CassandraConnector = _

  override def open(partitionId: Long, version: Long): Boolean = {
    cassandraConnector = CassandraConnector(sparkConf)
    true
  }

  override def close(errorOrNull: Throwable): Unit = {
  }

}
