val zioVersion        = "2.0.13"
val zioJsonVersion    = "0.5.0"
val zioLoggingVersion = "2.1.12"

val moduleCompilerOptions: Seq[String] = Seq(
  "-deprecation",            // Emit warning and location for usages of deprecated APIs.
  "-explaintypes",           // Explain type errors in more detail.
  "-feature",                // Emit warning and location for usages of features that should be imported explicitly.
  "-language:higherKinds",   // Allow higher-kinded types
  // "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Ywarn-dead-code",        // Warn when dead code is identified.
  "-Ywarn-value-discard",    // Annoying or useful?
  "-Xlint:constant",         // Evaluation of a constant arithmetic expression results in an error.
  "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
  "-Ywarn-unused:locals",    // Warn if a local definition is unused.
  "-Ywarn-unused:params",    // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars",   // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates",  // Warn if a private member is unused.
  "-Ywarn-extra-implicit",   // Warn when more than one implicit parameter section is defined.
  "-Xlint:missing-interpolator",
  "-Xsource:3",
  "-Ywarn-unused:imports"
)

val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "net.degoes",
        scalaVersion := "2.13.10",
        scalacOptions ++= moduleCompilerOptions
      )
    ),
    name           := "jvm-perf",
    libraryDependencies ++= Seq(
      // general
      "dev.zio" %% "zio-json" % zioJsonVersion,
      "dev.zio" %% "zio"      % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion,

      // logging
      "dev.zio" %% "zio-logging"       % zioLoggingVersion,
      "dev.zio" %% "zio-logging-slf4j" % zioLoggingVersion,

      // test
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .enablePlugins(JmhPlugin)
