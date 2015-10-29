
import AssemblyKeys._

assemblySettings

name := "spark-example"

organization := "eu.pepot.eu"

version := "0.1"

scalaVersion := "2.10.4"

checksums in update := Nil

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies += "org.specs2" %% "specs2" % "2.4.2" % "test"

libraryDependencies += "joda-time" % "joda-time" % "2.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"


resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
    case entry => {
      val strategy = mergeStrategy(entry)
      if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
      else strategy
    }
  }
}

javacOptions in Compile ++= Seq("-source", "1.6",  "-target", "1.6")

packageArchetype.java_application

net.virtualvoid.sbt.graph.Plugin.graphSettings
