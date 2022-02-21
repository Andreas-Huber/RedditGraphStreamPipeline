name := "redditdatasetstreampipeline"

version := "0.1"

scalaVersion := "2.13.8"

val AkkaVersion = "2.6.10"
val AlpakkaVersion = "2.0.2"

// Application Dependencies
libraryDependencies += "com.github.luben" % "zstd-jni" % "1.5.2-1"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.21"
libraryDependencies += "org.tukaani" % "xz" % "1.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-json-streaming" % AlpakkaVersion
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % AlpakkaVersion
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % AlpakkaVersion
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.6"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0"

// Test dependencies
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.3" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test


// JavaAppPackaging https://sbt-native-packager.readthedocs.io/en/latest/archetypes/java_app/index.html
enablePlugins(JavaAppPackaging)
Compile / mainClass := Some("no.simula.umod.redditdatasetstreampipeline.Main")
// discard automatically found main classes
Compile / discoveredMainClasses := Seq()
maintainer := "andreas.huber@infinite-coding.com"