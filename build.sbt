
// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.12"

name := "TopItemsByLocation"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"