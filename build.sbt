name := "etl-spark"
version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "com.databricks" %% "spark-redshift" % "3.0.0-preview1"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-redshift" % "1.11.467"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-core" % "1.11.467"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.467"
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.1"
//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.1"