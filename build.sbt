name := "redditdatasetstreampipeline"

version := "0.1"

scalaVersion := "2.13.3"



libraryDependencies += "com.github.luben" % "zstd-jni" % "1.4.5-6"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.20"
libraryDependencies += "org.tukaani" % "xz" % "1.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.8"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-json-streaming" % "2.0.1"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "2.0.1"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % "2.0.1"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"


