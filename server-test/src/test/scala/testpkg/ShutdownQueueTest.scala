/*
 * sbt
 * Copyright 2023, Scala center
 * Copyright 2011 - 2022, Lightbend, Inc.
 * Copyright 2008 - 2010, Mark Harrah
 * Licensed under Apache License 2.0 (see LICENSE)
 */

package testpkg

import scala.concurrent.duration.*
import sbt.io.IO
import sbt.io.syntax.*
import sbt.internal.langserver.{ LogMessageParams, SbtExecParams, SbtShutdownParams }
import sbt.internal.langserver.codec.JsonProtocol.given
import sbt.protocol.ExecStatusEvent
import sbt.protocol.codec.JsonProtocol.given

/**
 * A `shutdown` that is queued behind a running command closes the queue right away: any `sbt/exec`
 * sent after it, whether over raw JSON-RPC or from the thin client, is rejected with
 * `ServerShuttingDown` instead of being queued and dropped. This includes a second graceful
 * shutdown, which must not queue up behind the first one and gets the same error whether it is sent
 * as `sbt/exec shutdown` or as `sbt/shutdown`. The running command still completes, and the server then exits.
 */
class ShutdownQueueTest extends AbstractServerTest:
  override val testDirectory: String = "shutdown"

  test("a queued shutdown rejects later commands, including another shutdown, until exit") {
    val slowId = svr.session.nextId()
    svr.session.sendJsonRpc(slowId, "sbt/exec", SbtExecParams("gatedSlowTask")).get
    svr.session
      .waitForParamsInNotificationMsg[LogMessageParams](5.seconds) { p =>
        p.message.contains("gatedSlowTask started")
      }
      .get

    val shutdownId = svr.session.nextId()
    svr.session.sendJsonRpc(shutdownId, "sbt/exec", SbtExecParams("shutdown")).get
    svr.session
      .waitForParamsInNotificationMsg[ExecStatusEvent](10.seconds) { s =>
        s.status == "Queued" && s.message.contains("waiting for: gatedSlowTask")
      }
      .get

    val rejectedId = svr.session.nextId()
    svr.session.sendJsonRpc(rejectedId, "sbt/exec", SbtExecParams("quickTask")).get
    val error = svr.session.waitForResponseMsg(10.seconds, rejectedId).get.error
    assertRejectedByShutdown(error, "quickTask")

    val secondShutdownId = svr.session.nextId()
    svr.session.sendJsonRpc(secondShutdownId, "sbt/exec", SbtExecParams("shutdown")).get
    val secondShutdownError = svr.session.waitForResponseMsg(10.seconds, secondShutdownId).get.error
    assertRejectedByShutdown(secondShutdownError, "shutdown")

    val requestId = svr.session.nextId()
    svr.session.sendJsonRpc(requestId, "sbt/shutdown", SbtShutdownParams(Some(false))).get
    val requestError = svr.session.waitForResponseMsg(10.seconds, requestId).get.error
    assert(requestError == secondShutdownError, s"$requestError != $secondShutdownError")

    val (exitCode, lines) = runBatchClientWithOutput("quickTask")
    assert(exitCode == 1, lines.mkString("\n"))
    assert(
      lines.exists(l => l.contains("shutting down") && l.contains("'quickTask'")),
      lines.mkString("\n")
    )

    IO.touch(svr.baseDirectory / "release")
    val slowResponse =
      svr.session.waitForResultInResponseMsg[ExecStatusEvent](60.seconds, slowId).get
    assert(slowResponse.status == "Done")
    assert(waitUntil(60.seconds)(!svr.isAlive), "the server did not exit after the shutdown")
  }
end ShutdownQueueTest
