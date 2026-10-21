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
import sbt.internal.langserver.{ ErrorCodes, LogMessageParams, SbtExecParams, SbtShutdownParams }
import sbt.internal.langserver.codec.JsonProtocol.given
import sbt.internal.protocol.JsonRpcRequestMessage
import sbt.protocol.{ ExecStatusEvent, ServerSession }
import sbt.protocol.codec.JsonProtocol.given

/**
 * A graceful `sbt/shutdown` waits for the running command like `sbt/exec shutdown` does, and
 * a second one is rejected while the first is queued.
 */
class ShutdownRequestTest extends AbstractServerTest:
  override val testDirectory: String = "shutdown"
  private def portfile = svr.baseDirectory / "project" / "target" / "active.json"

  test("a graceful shutdown request queues behind the running command") {
    val slowId = svr.session.nextId()
    svr.session.sendJsonRpc(slowId, "sbt/exec", SbtExecParams("gatedSlowTask")).get
    svr.session
      .waitForParamsInNotificationMsg[LogMessageParams](5.seconds) { p =>
        p.message.contains("gatedSlowTask started")
      }
      .get

    val shutdownId = svr.session.nextId()
    svr.session.sendJsonRpc(JsonRpcRequestMessage("2.0", shutdownId, "sbt/shutdown", None)).get
    val queued =
      svr.session.waitForResultInResponseMsg[ExecStatusEvent](10.seconds, shutdownId).get
    assert(queued.status == "Queued", queued.toString)
    assert(queued.message.contains("waiting for: gatedSlowTask"), queued.toString)
    assert(queued.execId.exists(_.nonEmpty), queued.toString)

    val other = ServerSession.connect(portfile)
    val capabilities = other.initialize(10.seconds, subscribeToAll = false).get.capabilities
    assert(capabilities.shutdownRequest.contains(true), capabilities.toString)
    val secondId = other.nextId()
    other.sendJsonRpc(secondId, "sbt/shutdown", SbtShutdownParams(Some(false))).get
    val secondError = other.waitForResponseMsg(10.seconds, secondId).get.error
    assert(secondError.exists(_.code == ErrorCodes.ServerShuttingDown), secondError.toString)

    val rejectedId = svr.session.nextId()
    svr.session.sendJsonRpc(rejectedId, "sbt/exec", SbtExecParams("quickTask")).get
    val error = svr.session.waitForResponseMsg(10.seconds, rejectedId).get.error
    assert(error.exists(_.code == ErrorCodes.ServerShuttingDown), error.toString)

    IO.touch(svr.baseDirectory / "release")
    val slowResponse =
      svr.session.waitForResultInResponseMsg[ExecStatusEvent](60.seconds, slowId).get
    assert(slowResponse.status == "Done")
    assert(waitUntil(60.seconds)(!svr.isAlive), "the server did not exit after the shutdown")
  }
end ShutdownRequestTest
