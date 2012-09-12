package com.github.zhongl.codejam

import System.{currentTimeMillis => now}
import collection.mutable.ArrayBuffer
import actors.{Scheduler, Actor, Reactor}

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
  Scheduler.shutdown()

  private def countPrimeIn(num: Int, parallels: Int) = {
    var count = 1 // include num number: 2
    var finished = 0
    var running = true
    val workers = splitRange(3, num, parallels) map { new Worker(_, Actor.self) }

    while (running) {
      Actor.receive {
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

  private class Worker(range: (Int, Int), main: Actor) extends Reactor[Any] {
    val (from, to) = range
    val primes     = new ArrayBuffer[Int]
    var max        = 2

    @inline def findDivisibleOf(value: Int): Option[Int] = {
      val divisible = value % (_: Int) == 0
      primes find { divisible } orElse { max until value find { divisible } }
    }

    def finish() { main ! Finish; printf("Range(%1$d, %2$d) is over.\n", from, to); exit() }

    def act() {
      loop {
        react {
          case FoundPrime(n) if (n < to) => primes += n; max = math.max(max, n)
          case CheckPrime(n)             =>
            if (findDivisibleOf(n).isEmpty) main ! FoundPrime(n)
            if (n + 1 < to) this ! CheckPrime(n + 1) else finish()
        }
      }
    }

    start()
    Worker.this ! CheckPrime(from)
  }

  private def time[T](fun: => T) = {
    val begin = now
    val res = fun
    (now - begin, res)
  }

  private case class Finish()

  private case class FoundPrime(num: Int)

  private case class CheckPrime(num: Int)

}