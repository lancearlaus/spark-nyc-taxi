name := "spark-nyc-taxi"

organization := "com.lancearlaus"

version := "0.1"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"  % "1.5.1",
  "org.apache.spark"  %% "spark-sql"  % "1.5.1",
  "com.databricks"    %% "spark-csv"   % "1.2.0",

  "org.scalatest"     %% "scalatest"   % "2.2.1" % "test"
)

homepage := Some(url("https://github.com/lancearlaus/spark-nyc-taxi"))

licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

mainClass in (Compile, run) := Some("Main")