name := "spark-scala-sandbox"

version := "0.1"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.0"
val sparkCSVVersion = "1.5.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.databricks" % "spark-csv_2.10" % sparkCSVVersion,
  "org.apache.commons" % "commons-lang3" % "3.3.2"
)