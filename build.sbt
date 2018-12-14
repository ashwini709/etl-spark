name := "etl-spark"
version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "com.databricks" %% "spark-redshift" % "3.0.0-preview1"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-redshift" % "1.11.285"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"