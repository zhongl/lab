package com.github.zhongl.codejam

import System.{currentTimeMillis => now}
import actors.Actor._
import collection.mutable.ArrayBuffer
import actors.Actor

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
object ActorBasedPrime extends App {

  val (num, parallels) = args match {
    case Array(n)    => (n.toInt, sys.runtime.availableProcessors() * 2)
    case Array(n, f) => (n.toInt, sys.runtime.availableProcessors() * f.toInt)
    case _           => throw new IllegalArgumentException
  }

  val (elapse, count) = time { countPrimeIn(num, parallels) }
  printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, elapse)

  private def countPrimeIn(num: Int, parallels: Int) = {
    var count = 1 // include num number: 2
    var finished = 0
    var running = true

    val workers = splitRange(3, num, parallels) map { worker(_, self) }

    while (running) {
      receive {
        case FoundPrime(n) => workers foreach { _ ! FoundPrime(n) }; count += 1
        case Finish        => finished += 1; if (finished == parallels) running = false
      }
    }
    count
  }

  private def splitRange(from: Int, to: Int, parts: Int) = {
    val range = to - from
    val step = range / (parts - 1)

    def begin(i: Int) = from + (i * step)
    def end(i: Int) = math.min(begin(i) + step, to)

    0 until parts map { i => (begin(i), end(i)) }
  }

  private def worker(range: (Int, Int), main: Actor) = actor {
    val (from, to) = range
    val primes = ArrayBuffer(2)
    var max = 2

    @inline def findDivisibleOf(value: Int) = {
      val divisible = value % (_: Int) == 0
      primes find { divisible } orElse { max until value find { divisible } }
    }

    def finish() { main ! Finish; printf("Range(%1$d, %2$d) is over.\n", from, to); exit() }

    self ! CheckPrime(from)

    loop {
      react {
        case FoundPrime(n) if (n < to) => primes += n; max = math.max(max, n)
        case CheckPrime(n)             =>
          if (findDivisibleOf(n).isEmpty) main ! FoundPrime(n)
          if (n + 1 < to) self ! CheckPrime(n + 1) else finish()
      }
    }
  }

  private def time[T](fun: => T) = {
    val begin = now
    val res = fun
    (now - begin, res)
  }

  case class Finish()

  case class FoundPrime(num: Int)

  case class CheckPrime(num: Int)

}
