package com.github.zhongl.codejam

import java.util.{BitSet => Bits}
import annotation.tailrec
import System.{currentTimeMillis => now}
import actors.Actor._

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
object CountPrimeByParFilter extends App {
  val (num, parallels) = args match {
    case Array(n, p) => (n.toInt, p.toInt)
    case Array(n)    => (n.toInt, 8)
    case _           => throw new IllegalArgumentException("Usage: CMD <num> <parallels>")
  }

  val begin = now
  val count = countPrimeIn(num, parallels)
  printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, now - begin)

  def countPrimeIn(num: Int, parallels: Int) = {
    val maxValidateNum = math.sqrt(num).toInt + 1
    val main = self

    0 until parallels foreach {
      n => actor {
        val filter = new Bits(num)
        filter.set(0, 2) // 0,1 is not a prime

        @tailrec def setNonPrimeByProductOf(i: Int, j: Int): Option[_] = i * j match {
          case index if (index < num) => filter.set(index); setNonPrimeByProductOf(i, j + 1)
          case _                      => None
        }

        (2 + n) until maxValidateNum by parallels foreach { i => setNonPrimeByProductOf(i, i) }
        main ! filter
      }
    }

    @inline def income = receive { case f: Bits => f }

    @tailrec def reduce(filter: Bits, finished: Int): Bits =
      if (finished == parallels) filter else {filter or income; reduce(filter, finished + 1) }

    num - reduce(income, 1).cardinality()
  }
}
