name := "redditdatasetstreampipeline"

version := "0.1"

scalaVersion := "2.13.3"

val AkkaVersion = "2.6.9"
val AlpakkaVersion = "2.0.2"

libraryDependencies += "com.github.luben" % "zstd-jni" % "1.4.5-6"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.20"
libraryDependencies += "org.tukaani" % "xz" % "1.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-json-streaming" % "2.0.2"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "2.0.2"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % "2.0.2"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test


assemblyJarName in assembly := "rdsp.jar"

mainClass in assembly := Some("no.simula.umod.redditdatasetstreampipeline.Main")

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

