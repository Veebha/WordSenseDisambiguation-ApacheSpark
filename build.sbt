name := "WordSenseDisambiguation"

version := "1.0"

scalaVersion := "2.10.5"

sbtVersion := "0.10.5"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.9.37"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.0"

libraryDependencies += "org.apache.spark" % "spark-bagel_2.10" % "1.2.1"

libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.13"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.13"