
name := "Bot Detector"

version := "0.1"

scalaVersion := "2.11.8"

sparkVersion := "2.3.1"

sparkComponents ++= Seq("sql", "catalyst", "streaming")

spDependencies += "datastax/spark-cassandra-connector:2.3.1-s_2.11"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"

libraryDependencies += "org.apache.ignite" % "ignite-spark" % "2.6.0"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.6-M4"

assemblyJarName in assembly := s"${name.value.replace(' ', '-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}