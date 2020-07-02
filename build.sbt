name := "scalazstd"

version := "0.1"

scalaVersion := "2.13.2"

libraryDependencies += "com.github.luben" % "zstd-jni" % "1.4.5-4"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.20"
libraryDependencies += "org.tukaani" % "xz" % "1.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.6"
