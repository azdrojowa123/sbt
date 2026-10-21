/*
 * sbt
 * Copyright 2023, Scala center
 * Copyright 2011 - 2022, Lightbend, Inc.
 * Copyright 2008 - 2010, Mark Harrah
 * Licensed under Apache License 2.0 (see LICENSE)
 */

package sbt
package internal

import hedgehog.*
import hedgehog.runner.*
import _root_.sbt.BasicCommandStrings.Shutdown
import CommandExchange.ShutdownPhase
import CommandExchange.ShutdownPhase.*

object CommandExchangeSpec extends Properties:
  override def tests: List[Test] = List(
    property("a queue without a shutdown keeps accepting commands", openQueueAccepts),
    property("a shutdown anywhere in the queue closes it", shutdownAnywhereCloses),
    property("a shutdown inside a compound command does not close the queue", compoundKeepsOpen),
    example("a shutdown counts whatever its source is", anySourceCounts),
    example("a command longer than 50 characters is cut in the waiting message", longCommandIsCut),
    property("a shutdown never goes back to an earlier phase", phaseNeverGoesBack),
    property("a request moves the shutdown forward or changes nothing", phaseMovesForwardOrStays),
    example("only a force passes a pending shutdown, and nothing passes a force", phaseRules),
  )

  private val plainCommand: Gen[String] =
    Gen.element1("compile", "test", "quickTask", "slowTask", "reboot", "exit")

  private val compoundShutdown: Gen[String] =
    for
      parts <- plainCommand.list(Range.linear(1, 3))
      index <- Gen.int(Range.linear(0, parts.length))
      leading <- Gen.element1("", ";")
      separator <- Gen.element1(";", "; ")
    yield leading + parts.patch(index, List(Shutdown), 0).mkString(separator)

  private val source: Gen[Option[CommandSource]] =
    Gen.choice1(
      Gen.constant(None),
      Gen.int(Range.linear(0, 5)).map(n => Some(CommandSource(s"network-$n"))),
      Gen.constant(Some(CommandSource("console0")))
    )

  private def exec(commandLine: Gen[String]): Gen[Exec] =
    for
      cmd <- commandLine
      src <- source
    yield Exec(cmd, Some(Exec.newExecId), src)

  private val openQueue: Gen[List[Exec]] = exec(plainCommand).list(Range.linear(0, 10))

  private def openQueueWith(commandLine: Gen[String]): Gen[List[Exec]] =
    for
      queue <- openQueue
      inserted <- exec(commandLine)
      index <- Gen.int(Range.linear(0, queue.length))
    yield queue.patch(index, List(inserted), 0)

  private val closedQueue: Gen[List[Exec]] = openQueueWith(Gen.constant(Shutdown))

  private val compoundQueue: Gen[List[Exec]] = openQueueWith(compoundShutdown)

  def openQueueAccepts: Property =
    openQueue.forAll.map(queue => CommandExchange.hasQueuedShutdown(queue) ==== false)

  def shutdownAnywhereCloses: Property =
    closedQueue.forAll.map(queue => CommandExchange.hasQueuedShutdown(queue) ==== true)

  def compoundKeepsOpen: Property =
    compoundQueue.forAll.map(queue => CommandExchange.hasQueuedShutdown(queue) ==== false)

  def anySourceCounts: Result =
    Result.all(
      List(None, Some(CommandSource("console0")), Some(CommandSource("network-1"))).map: src =>
        CommandExchange.hasQueuedShutdown(List(Exec(Shutdown, src))) ==== true
    )

  private val phase: Gen[ShutdownPhase] = Gen.element1(Idle, Graceful, Forced, Started)

  /** Applies `requests` one after another, as [[CommandExchange]] does for each shutdown request. */
  private def phaseAfter(requests: List[ShutdownPhase]): ShutdownPhase =
    requests.foldLeft(Idle)(ShutdownPhase.advance)

  def phaseNeverGoesBack: Property =
    for
      requests <- phase.list(Range.linear(0, 10)).forAll
      next <- phase.forAll
    yield
      val before = phaseAfter(requests)
      Result.assert(ShutdownPhase.advance(before, next).ordinal >= before.ordinal)

  def phaseMovesForwardOrStays: Property =
    for
      requests <- phase.list(Range.linear(0, 10)).forAll
      next <- phase.forAll
    yield
      val before = phaseAfter(requests)
      val after = ShutdownPhase.advance(before, next)
      Result.assert(after == before || after == next)

  def phaseRules: Result =
    def accepts(current: ShutdownPhase, requested: ShutdownPhase): Boolean =
      ShutdownPhase.advance(current, requested) != current
    Result.all(
      List(
        accepts(Idle, Graceful) ==== true,
        accepts(Idle, Forced) ==== true,
        accepts(Graceful, Graceful) ==== false,
        accepts(Graceful, Forced) ==== true,
        accepts(Forced, Graceful) ==== false,
        accepts(Forced, Forced) ==== false,
        accepts(Started, Graceful) ==== false,
        accepts(Started, Forced) ==== false,
      )
    )

  def longCommandIsCut: Result =
    val short = "a" * 50
    Result.all(
      List(
        CommandExchange.waitingForMessage(short) ==== s"waiting for: $short",
        CommandExchange.waitingForMessage(short + "b") ==== s"waiting for: $short...",
      )
    )
end CommandExchangeSpec
