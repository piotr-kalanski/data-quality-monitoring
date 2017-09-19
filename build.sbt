name := "data-quality-monitoring"

organization := "com.github.piotr-kalanski"

version := "0.3.2"

scalaVersion := "2.11.8"

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("https://github.com/piotr-kalanski/data-quality-monitoring"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/piotr-kalanski/data-quality-monitoring"),
    "scm:git:ssh://github.com/piotr-kalanski/data-quality-monitoring.git"
  )
)

developers := List(
  Developer(
    id    = "kalan",
    name  = "Piotr Kalanski",
    email = "piotr.kalanski@gmail.com",
    url   = url("https://github.com/piotr-kalanski")
  )
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "com.github.piotr-kalanski" %% "es-client" % "0.2.1",
  "com.typesafe" % "config" % "1.3.0",
  "com.github.piotr-kalanski" %% "class2sql" % "0.1.4",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.h2database" % "h2" % "1.4.195" % "test"
)

coverageExcludedPackages := "com.datawizards.dqm.examples.*"

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
