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
import sbt.internal.langserver.{ ErrorCodes, SbtExecParams, SbtShutdownParams }
import sbt.internal.langserver.codec.JsonProtocol.given
import sbt.protocol.{ ExecStatusEvent, ServerSession }
import sbt.protocol.codec.JsonProtocol.given

/**
 * A forced `sbt/shutdown` cannot interrupt a running command, only a task, so it waits for the
 * command to finish. While it waits, it still rejects the queued commands and a second forced
 * request. The server exits once the command completes.
 */
class ShutdownForceCommandTest extends AbstractServerTest:
  override val testDirectory: String = "shutdown"
  private def portfile = svr.baseDirectory / "project" / "target" / "active.json"

  private def connect(): ServerSession =
    val session = ServerSession.connect(portfile)
    session.initialize(10.seconds, subscribeToAll = false).get
    session

  test("a forced shutdown waits for a running command and rejects a second force") {
    val slowId = svr.session.nextId()
    svr.session.sendJsonRpc(slowId, "sbt/exec", SbtExecParams("gatedSlowCommand")).get
    assert(
      waitUntil(10.seconds)((svr.baseDirectory / "started").exists),
      "gatedSlowCommand did not start"
    )

    val queuedClient = connect()
    val queuedId = queuedClient.nextId()
    queuedClient.sendJsonRpc(queuedId, "sbt/exec", SbtExecParams("quickTask")).get
    queuedClient
      .waitForParamsInNotificationMsg[ExecStatusEvent](10.seconds) { s =>
        s.execId.contains(queuedId) && s.status == "Queued"
      }
      .get

    val forcingClient = connect()
    val forceId = forcingClient.nextId()
    forcingClient.sendJsonRpc(forceId, "sbt/shutdown", SbtShutdownParams(Some(true))).get
    val forced =
      forcingClient.waitForResultInResponseMsg[ExecStatusEvent](10.seconds, forceId).get
    assert(forced.status == "Processing", forced.toString)

    val error = queuedClient.waitForResponseMsg(10.seconds, queuedId).get.error
    assertRejectedByShutdown(error, "quickTask")

    val lateClient = connect()
    val lateId = lateClient.nextId()
    lateClient.sendJsonRpc(lateId, "sbt/shutdown", SbtShutdownParams(Some(true))).get
    val lateError = lateClient.waitForResponseMsg(10.seconds, lateId).get.error
    assert(lateError.exists(_.code == ErrorCodes.ServerShuttingDown), lateError.toString)
    assert(lateError.exists(_.message.contains("forced shutdown was rejected")), lateError.toString)

    assert(svr.isAlive, "the server exited while the command was still running")
    IO.touch(svr.baseDirectory / "release")
    val slowResponse =
      svr.session.waitForResultInResponseMsg[ExecStatusEvent](15.seconds, slowId).get
    assert(slowResponse.status == "Done", slowResponse.toString)

    assert(shutdownNotification(forcingClient) == (false, None), "the first force did not run")
    assert(shutdownNotification(lateClient) == (true, None), "the rejected force ran instead")
    assert(waitUntil(60.seconds)(!svr.isAlive), "the server did not exit after the shutdown")
  }
end ShutdownForceCommandTest
