name := "stream-s3-wrapper"

version := "2.8.4"

scalaVersion := "2.12.2"
crossScalaVersions := Seq("2.11.8", "2.12.2")

organization := "io.dronekit"

resolvers += "Artifactory" at "https://dronekit.artifactoryonline.com/dronekit/libs-snapshot-local/"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

isSnapshot := true

publishTo := {
  val artifactory = "https://dronekit.artifactoryonline.com/"
  if (isSnapshot.value)
    Some("snapshots" at artifactory + s"dronekit/libs-snapshot-local;build.timestamp=${new java.util.Date().getTime}")
  else
    Some("snapshots" at artifactory + "dronekit/libs-release-local")
}

libraryDependencies ++= {
  val akkaV = "2.5.0"
  val akkaHttpV = "10.0.5"
  
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,

    "com.typesafe.akka" %% "akka-http-core" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV,

    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.122",
    "com.amazonaws" % "aws-java-sdk-core" % "1.11.122",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
}
