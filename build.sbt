organization := "com.despegar.p13n"
name := "kafka-tools"
version := "0.0"

scalaVersion := "2.12.5"

val jacksonVersion = "2.9.5"

mainClass in assembly := Some("com.despegar.p13n.kafka.tools.KafkaTools")
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

publishTo := {
  if (version.value.endsWith("SNAPSHOT"))
    Some("Nexus snapshots" at "http://nexus.despegar.it:8080/nexus/content/repositories/snapshots/")
  else
    Some("Nexus releases" at "http://nexus.despegar.it:8080/nexus/content/repositories/releases/")
}

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.0",
  "commons-io" % "commons-io" % "2.6",

  //Jackson
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion exclude("com.google.guava", "guava"),
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVersion exclude("com.google.guava", "guava"),

  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.0" % Test,
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)