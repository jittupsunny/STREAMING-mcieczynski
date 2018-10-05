package pl.mcieszynski.gridu.detector.events

import java.util.UUID

trait BaseEvent {
  def uuid: UUID

  def timestamp: Long

  def categoryId: Int

  def eventType: String
}
