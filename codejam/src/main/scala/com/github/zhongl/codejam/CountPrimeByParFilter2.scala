package com.github.zhongl.codejam

import java.util.{BitSet => Bits}
import annotation.tailrec
import System.{currentTimeMillis => now}
import actors.Actor._

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
object CountPrimeByParFilter2 extends App {
  val (num, parallels) = args match {
    case Array(n, p) => (n.toInt, p.toInt)
    case Array(n)    => (n.toInt, 8)
    case _           => throw new IllegalArgumentException("Usage: CMD <num> <parallels>")
  }

  val begin = now
  val count = countPrimeIn(num, parallels)
  printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, now - begin)

  private def countPrimeIn(num: Int, parallels: Int) = {
    if ((num - 3) / parallels <= 1) {
      countPrimeBetween(3, num)
    } else {
      val main = self

      splitRange(3, num, parallels) { (b, e) => actor { main ! countPrimeBetween(b, e) } }

      @tailrec def reduce(count: Int, finished: Int): Int =
        if (finished == parallels) count else reduce(count + ?.asInstanceOf[Int], finished + 1)

      reduce(?.asInstanceOf[Int], 1)
    } + 1 // add prime number 2
  }

  private def countPrimeBetween(from: Int, to: Int) = {
    val filter = new java.util.BitSet(to - from)

    @tailrec def setNonPrimeBy(i: Int, j: Int): Option[_] = i * j match {
      case n if (n > to - 1)        => None
      case n if (n < from || j < i) => setNonPrimeBy(i, j + 1)
      case n                        => filter.set(n - from); setNonPrimeBy(i, j + 1)
    }

    @tailrec def start(i: Int): Option[_] = if (i < to / 2) {
      setNonPrimeBy(i, from / i)
      start(i + 1)
    } else None

    start(2)
    to - from - filter.cardinality()
  }

  private def splitRange(from: Int, to: Int, parts: Int)(fun: (Int, Int) => Unit) {
    val range = to - from
    val step = range / (parts - 1)

    def begin(i: Int) = i * step + from
    def end(i: Int) = math.min(begin(i) + step, to)

    0 until parts foreach { i => fun(begin(i), end(i)) }
  }
}
