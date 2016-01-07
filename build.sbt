import spray.revolver.RevolverPlugin.Revolver

@inline def env(n: String): Option[String] = sys.env.get(n)

val uDataVersion = "0.0.7-SNAPSHOT"

lazy val commonSettings = Seq(
  organization := "com.udata",
  version := "0.0.11-SNAPSHOT",
  scalaVersion := "2.11.7",
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:existentials"),
  publishTo := {
    if (isSnapshot.value)
      Some(Resolver.file("file", new File(env("FILE_PUBLISH").getOrElse(Path.userHome.getAbsolutePath + "/Dropbox/Public/maven/") + "snapshot")))
    else
      Some(Resolver.file("file", new File(env("FILE_PUBLISH").getOrElse(Path.userHome.getAbsolutePath + "/Dropbox/Public/maven/") + "release")))
  },
  libraryDependencies := Seq(
    "com.udata"                %% "structures"     % uDataVersion,
    "org.scalatest"            %% "scalatest"      % "2.2.4"           % "test",
    "com.google.code.findbugs" %  "jsr305"         % "3.0.1"           % "test",
    "com.github.docker-java"   %  "docker-java"    % "3.0.0-SNAPSHOT"  % "test"
  )
)

lazy val mongo = (project in file("mongo")).
  settings(
    fork in Test := true,
    parallelExecution in Test := false
  ).
  settings(commonSettings).
  settings(
    libraryDependencies ++= Seq(
      "org.reactivemongo"      %% "reactivemongo"  % "0.11.8"
    )
  )

lazy val aws = (project in file("aws")).
  settings(
    fork in Test := false,
    parallelExecution in Test := false
  ).
  settings(commonSettings).
  settings(
    libraryDependencies ++= Seq(
      "com.github.seratch"      %% "awscala"        % "0.5.5"
    )
  )


lazy val mongoExample = (project in file("mongo-example")).
  settings(commonSettings).
  settings(
    libraryDependencies ++= Seq(
      "com.udata"               %% "hub"            % uDataVersion
    )
  ).
  settings(Revolver.settings).
  dependsOn(mongo)
