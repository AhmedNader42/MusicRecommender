ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "MusicRecommender",
    idePackagePrefix := Some("org.ahmed.MusicRecommender")
  )

val spark_version = "3.5.1"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,
  "org.apache.spark" %% "spark-mllib" % spark_version)