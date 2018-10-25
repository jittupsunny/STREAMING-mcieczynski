package pl.mcieszynski.gridu.detector.events

trait BaseEvent {
  def uuid: String

  def timestamp: Long

  def categoryId: Int

  def eventType: String
}
