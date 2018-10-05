package pl.mcieszynski.gridu.detector

trait DetectorServiceTestConstants {

  val kafkaMessageUUID = "topic_partitionId_offsetId"

  val timestamp = 1538648621

  val categoryId = 1005

  val ip = "172.10.13.247"

  val eventType = "view"

  val validEventJson = "{\"unix_time\": " + 1538648621 + ", \"category_id\": " + categoryId +
    ", \"ip\": \"" + ip + "\", \"type\": \"" + eventType + "\"}"

}
