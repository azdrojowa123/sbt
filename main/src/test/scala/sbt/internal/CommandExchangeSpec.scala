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

object CommandExchangeSpec extends Properties:
  override def tests: List[Test] = List(
    property("a queue without a shutdown keeps accepting commands", openQueueAccepts),
    property("a shutdown anywhere in the queue closes it", shutdownAnywhereCloses),
    property("a shutdown inside a compound command does not close the queue", compoundKeepsOpen),
    example("a shutdown counts whatever its source is", anySourceCounts),
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
end CommandExchangeSpec
