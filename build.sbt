@inline def env(n: String): Option[String] = sys.env.get(n)

lazy val commonSettings = Seq(
  organization := "com.kperson",
  version := "0.0.2-SNAPSHOT",
  scalaVersion := "2.11.7",
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  publishTo := {
    if (isSnapshot.value)
      Some(Resolver.file("file", new File(env("FILE_PUBLISH").getOrElse(Path.userHome.getAbsolutePath + "/Dropbox/Public/maven/") + "snapshot")))
    else
      Some(Resolver.file("file", new File(env("FILE_PUBLISH").getOrElse(Path.userHome.getAbsolutePath + "/Dropbox/Public/maven/") + "release")))
  },
  libraryDependencies := Seq(
    "com.kperson"              %% "structures"     % "0.0.17-SNAPSHOT",
    "org.scalatest"            %% "scalatest"      % "2.2.4"           % "test",
    "com.google.code.findbugs" %  "jsr305"         % "3.0.1"           % "test",
    "com.github.docker-java"   %  "docker-java"    % "3.0.0-SNAPSHOT"  % "test"

  )
)

lazy val udatamongo = (project in file("mongo")).
  settings(
    fork in Test := true,
    parallelExecution in Test := false
  ).
  settings(commonSettings).
  settings(libraryDependencies ++= Seq(
    "org.reactivemongo"       %% "reactivemongo"  % "0.11.8"
  ))
