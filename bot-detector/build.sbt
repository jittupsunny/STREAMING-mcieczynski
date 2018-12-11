import sbt.Keys.libraryDependencies

name := "Bot Detector"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

libraryDependencies ++= sparkDependencies ++ testDependencies ++ kafkaDependencies ++ coreDependencies

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion
)

// according to known issue: https://github.com/sbt/sbt/issues/3618
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

// Core Dependencies
lazy val coreDependencies = Seq(
  "org.apache.ignite" % "ignite-spark" % "2.6.0",
  "net.liftweb" %% "lift-json" % "2.6-M4",
  "org.slf4j" % "slf4j-simple" % "1.7.25"
)

// Kafka dependencies
lazy val kafkaDependencies = Seq(
  "org.apache.kafka" % "kafka-clients" % "2.0.0"
)

//Test Dependencies
lazy val testDependencies = Seq(
  "com.typesafe" % "config" % "1.3.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalamock" %% "scalamock-core" % "3.1.1" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.1.1" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % Test,
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % Test,
  "net.manub" %% "scalatest-embedded-kafka-streams" % "2.0.0" % Test,
  "org.mockito" % "mockito-all" % "1.10.19" % Test,
  "org.apache.hive" % "hive-exec" % "2.3.1" % Test,
  "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test
)

resolvers += Resolver.mavenLocal
resolvers += "Cascading repo" at "http://conjars.org/repo"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
dependencyOverrides += "org.apache.kafka" % "kafka-clients" % "2.0.0"

assemblyJarName in assembly := s"${name.value.replace(' ', '-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}