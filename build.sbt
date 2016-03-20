import AssemblyKeys._

assemblySettings

name := "spark-input-splitter"

organization := "eu.pepot.eu"

version := "0.1"

scalaVersion := "2.10.4"

checksums in update := Nil

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.3.0_0.2.0" % "test"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.7" % "test"

libraryDependencies += "org.mockito" % "mockito-all" % "1.9.5" % "test"


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

parallelExecution in Test := false

packageArchetype.java_application

net.virtualvoid.sbt.graph.Plugin.graphSettings
