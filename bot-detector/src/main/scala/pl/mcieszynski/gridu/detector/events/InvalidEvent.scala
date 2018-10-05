package pl.mcieszynski.gridu.detector.events

case class InvalidEvent(originalMessage: String, exception: Throwable)
