import AssemblyKeys._

assemblySettings

name := "dbtools"

version := "0.1-SNAPSHOT"

assemblyOption in assembly ~= { _.copy(prependShellScript = Some(defaultShellScript)) }

jarName in assembly := { s"${name.value}-${version.value}" }

buildInfoSettings

sourceGenerators in Compile <+= buildInfo

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "buildinfo"

scalaVersion := "2.11.1"

scalacOptions in ThisBuild ++= Seq(
  "-target:jvm-1.6",
  "-encoding", "UTF-8",
  "deprecation",        // warning and location for usages of deprecated APIs
  "-feature",           // warning and location for usages of features that should be imported explicitly
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-unchecked",          // additional warnings where generated code depends on assumptions
//  "-Xlint",
//  "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver
  "-Ywarn-dead-code",
  "-Ywarn-inaccessible",
  "-Ywarn-value-discard" // Warn when non-Unit expression results are unused
)

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)


libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging"        % "3.0.0",
  "com.github.tototoshi"       %% "scala-csv"            % "1.0.0",
  "ch.qos.logback"             %  "logback-classic"      % "1.0.1",
  "com.github.nscala-time"     %% "nscala-time"          % "1.2.0",
  "mysql"                      %  "mysql-connector-java" % "5.1.28",
  "com.typesafe.slick"         %% "slick"                % "2.1.0",
  "com.typesafe"               %  "config"               % "1.2.1",
  "com.github.tototoshi"       %% "slick-joda-mapper"    % "1.2.0"
)

// allows calling System.exit() from application
fork in run := true
