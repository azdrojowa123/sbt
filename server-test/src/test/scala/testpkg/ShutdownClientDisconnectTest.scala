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
import sbt.internal.langserver.{ LogMessageParams, SbtExecParams }
import sbt.internal.langserver.codec.JsonProtocol.given
import sbt.protocol.{ ExecStatusEvent, ServerSession }
import sbt.protocol.codec.JsonProtocol.given

/**
 * A client that queued a shutdown and then went away does not take the shutdown with it: the
 * queue stays closed and the server still exits once the running command is done.
 */
class ShutdownClientDisconnectTest extends AbstractServerTest:
  override val testDirectory: String = "shutdown"
  private def portfile = svr.baseDirectory / "project" / "target" / "active.json"

  test("a queued shutdown survives its client disconnecting and keeps the queue closed") {
    val slowId = svr.session.nextId()
    svr.session.sendJsonRpc(slowId, "sbt/exec", SbtExecParams("gatedSlowTask")).get
    svr.session
      .waitForParamsInNotificationMsg[LogMessageParams](5.seconds) { p =>
        p.message.contains("gatedSlowTask started")
      }
      .get

    val other = ServerSession.connect(portfile)
    other.initialize(10.seconds, subscribeToAll = false).get
    other.sendJsonRpc(other.nextId(), "sbt/exec", SbtExecParams("shutdown")).get
    other
      .waitForParamsInNotificationMsg[ExecStatusEvent](10.seconds) { s =>
        s.status == "Queued" && s.message.contains("waiting for: gatedSlowTask")
      }
      .get
    other.close()
    Thread.sleep(1000)

    val rejectedId = svr.session.nextId()
    svr.session.sendJsonRpc(rejectedId, "sbt/exec", SbtExecParams("quickTask")).get
    val error = svr.session.waitForResponseMsg(5.seconds, rejectedId).get.error
    assertRejectedByShutdown(error, "quickTask")

    IO.touch(svr.baseDirectory / "release")
    val slowResponse =
      svr.session.waitForResultInResponseMsg[ExecStatusEvent](60.seconds, slowId).get
    assert(slowResponse.status == "Done")
    assert(waitUntil(60.seconds)(!svr.isAlive), "the server did not exit after the shutdown")
  }
end ShutdownClientDisconnectTest
