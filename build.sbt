import bintray.Keys._

lazy val root = (project in file(".")).settings(
  sbtPlugin := true,
  
  name := "sbt-spark-ec2",
  version := "0.3.0",
  organization := "net.pishen",

  scalaVersion := "2.10.5",
  
  addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0"),
  libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.0-M2",
  
  publishMavenStyle := false,
  bintrayPublishSettings,
  repository in bintray := "sbt-plugins",
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  bintrayOrganization in bintray := None
)
