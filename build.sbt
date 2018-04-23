name := "kafka-tools"

version := "0.1"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.0" % Test,
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)