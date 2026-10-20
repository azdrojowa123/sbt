@transient
lazy val gatedSlowTask =
  taskKey[Unit]("Runs until the test creates the release file, so a shutdown can queue behind it")

@transient
lazy val quickTask = taskKey[Unit]("Finishes immediately")

gatedSlowTask := Def.uncached {
  val release = baseDirectory.value / "release"
  val deadline = System.currentTimeMillis + 120000
  streams.value.log.info("gatedSlowTask started")
  while !release.exists do
    if System.currentTimeMillis > deadline then sys.error("gatedSlowTask was never released")
    Thread.sleep(100)
}

quickTask := ()
