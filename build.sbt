name := "S3UploadSink"

version := "1.0"

scalaVersion := "2.11.7"

organization := "io.dronekit"


libraryDependencies ++= {
  val akkaV = "2.3.12"
  val akkaStreamV = "1.0"
  val scalaTestV = "2.2.4"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-core-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-testkit-experimental" % akkaStreamV,
    "com.amazonaws" % "aws-java-sdk-s3" % "1.10.11",
    "joda-time" % "joda-time" % "2.8.2",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )
}