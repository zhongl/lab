import sbt._
import sbt.Keys._

object CodejamBuild extends Build {

  lazy val codejam = Project(
    id = "codejam",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "codejam",
      organization := "com.github.zhongl",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2",
      resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" % "akka-actor" % "2.0.3",
        "org.mockito" % "mockito-all" % "1.9.0" % "test",
        "org.scalatest" %% "scalatest" % "1.7.2" % "test"
      )
      // add other settings here
    )
  )
}
