name := "Credit Card Fraud Detection"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"
libraryDependencies +=  "org.apache.kafka" % "kafka-clients" % "0.11.0.1"