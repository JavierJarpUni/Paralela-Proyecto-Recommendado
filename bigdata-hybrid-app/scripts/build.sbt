// build.sbt
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "bigdata-actor-system",
    libraryDependencies ++= Seq(
      // Akka
      "com.typesafe.akka" %% "akka-actor" % "2.6.20",
      "com.typesafe.akka" %% "akka-http" % "10.2.10",
      "com.typesafe.akka" %% "akka-stream" % "2.6.20",
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.10",
      
      // Spark
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      
      // Google Cloud
      "com.google.cloud" % "google-cloud-storage" % "2.22.3",
      "com.google.cloud" % "google-cloud-dataproc" % "4.13.0",
      "com.google.cloud" % "google-cloud-functions" % "0.6.0",
      
      // JSON
      "org.json4s" %% "json4s-jackson" % "4.0.6",
      "io.spray" %% "spray-json" % "1.3.6",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "com.typesafe.akka" %% "akka-testkit" % "2.6.20" % Test
    )
  )