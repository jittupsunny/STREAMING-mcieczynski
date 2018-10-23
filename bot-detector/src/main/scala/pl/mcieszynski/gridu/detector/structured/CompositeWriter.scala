package pl.mcieszynski.gridu.detector.structured

import org.apache.spark.sql.ForeachWriter

class CompositeWriter[T](writers: Seq[ForeachWriter[T]]) extends ForeachWriter[T] {

  override def open(partitionId: Long, version: Long): Boolean = {
    writers.forall(writer => writer.open(partitionId, version))
  }

  override def process(value: T): Unit = {
    writers.foreach(writer => writer.process(value))
  }

  override def close(errorOrNull: Throwable): Unit = {
    writers.foreach(writer => writer.close(errorOrNull))
  }
}
