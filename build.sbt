name := "simtext4S with spark"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.specs2" %% "specs2-core" % "3.7.2" % "test"
)

scalacOptions in Test ++= Seq("-Yrangepos")
