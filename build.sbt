name := "SparkHandsOn"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"

resolvers += "Job Server Binary" at "https://dl.bintray.com/spark-jobserver/maven"
libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.6.2" % "provided"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
    