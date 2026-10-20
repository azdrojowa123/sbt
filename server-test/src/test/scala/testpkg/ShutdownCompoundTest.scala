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
import sbt.protocol.ExecStatusEvent
import sbt.protocol.codec.JsonProtocol.given

/**
 * A queued compound command that contains `shutdown` does not close the queue, because it can
 * stop before its `shutdown` part runs. The queue closes only when that part starts.
 */
class ShutdownCompoundTest extends AbstractServerTest:
  override val testDirectory: String = "shutdown"

  private def waitForQueued(id: String): ExecStatusEvent =
    svr.session
      .waitForParamsInNotificationMsg[ExecStatusEvent](10.seconds): s =>
        s.status == "Queued" && s.execId.contains(id) &&
          s.message.contains("waiting for: gatedSlowTask")
      .get

  test("a queued compound with shutdown does not reject commands until its shutdown part starts"):
    val slowId = svr.session.nextId()
    svr.session.sendJsonRpc(slowId, "sbt/exec", SbtExecParams("gatedSlowTask")).get
    svr.session
      .waitForParamsInNotificationMsg[LogMessageParams](5.seconds): p =>
        p.message.contains("gatedSlowTask started")
      .get

    val compoundId = svr.session.nextId()
    svr.session.sendJsonRpc(compoundId, "sbt/exec", SbtExecParams(";quickTask;shutdown")).get
    waitForQueued(compoundId)

    val acceptedId = svr.session.nextId()
    svr.session.sendJsonRpc(acceptedId, "sbt/exec", SbtExecParams("quickTask")).get
    waitForQueued(acceptedId)

    IO.touch(svr.baseDirectory / "release")
    val slowResponse =
      svr.session.waitForResultInResponseMsg[ExecStatusEvent](60.seconds, slowId).get
    assert(slowResponse.status == "Done")

    val error = svr.session.waitForResponseMsg(30.seconds, acceptedId).get.error
    assertRejectedByShutdown(error, "quickTask")
    assert(waitUntil(60.seconds)(!svr.isAlive), "the server did not exit after the shutdown")
end ShutdownCompoundTest
