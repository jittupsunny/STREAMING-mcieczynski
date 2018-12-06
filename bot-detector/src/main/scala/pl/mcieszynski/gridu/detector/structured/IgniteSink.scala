package pl.mcieszynski.gridu.detector.structured

import org.apache.ignite.IgniteCache
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.ForeachWriter
import pl.mcieszynski.gridu.detector.events.AggregatedIpInformation

class IgniteSink(sparkContext: SparkContext, ignitePath: String, igniteCacheName: String) extends ForeachWriter[(String, AggregatedIpInformation)] {
  var igniteContext: IgniteContext = _
  var igniteCache: IgniteCache[String, AggregatedIpInformation] = _

  override def open(partitionId: Long, version: Long): Boolean = {
    igniteContext = new IgniteContext(sparkContext, ignitePath)
    igniteCache = igniteContext.fromCache(igniteCacheName).asInstanceOf[IgniteCache[String, AggregatedIpInformation]]
    true
  }

  override def process(value: (String, AggregatedIpInformation)): Unit = {
    igniteCache.put(value._1, value._2)
  }

  override def close(errorOrNull: Throwable): Unit = {
    igniteCache.close()
    igniteContext.close(true)
  }

}
