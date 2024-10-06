ThisBuild / scalaVersion := "3.4.2"

lazy val root = (project in file("."))
  .settings(
    name := "PipelineScala",
    version := "1.0",
  libraryDependencies ++= Seq(
  "com.softwaremill.sttp.client3" %% "core" % "3.6.0",
  "io.circe" %% "circe-core" % "0.14.3",
  "io.circe" %% "circe-generic" % "0.14.3",
  "io.circe" %% "circe-parser" % "0.14.3",
  "org.postgresql" % "postgresql" % "42.6.0",
  "org.apache.kafka" % "kafka-clients" % "2.8.1"
   )
  )

