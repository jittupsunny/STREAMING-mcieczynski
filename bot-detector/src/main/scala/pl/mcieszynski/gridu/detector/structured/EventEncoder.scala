package pl.mcieszynski.gridu.detector.structured

import org.apache.spark.sql.Encoders
import pl.mcieszynski.gridu.detector.events.Event

object EventEncoder {
  implicit def eventEncoder: org.apache.spark.sql.Encoder[Event] = Encoders.kryo(Event.getClass)
}
