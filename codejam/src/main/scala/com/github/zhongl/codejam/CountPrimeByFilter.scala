package com.github.zhongl.codejam

import java.util
import annotation.tailrec
import System.{currentTimeMillis => now}

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
object CountPrimeByFilter extends App {
  val num = args match {
    case Array(n) => n.toInt
    case _        => throw new IllegalArgumentException("Usage: CMD <num>")
  }

  val begin = now
  val count = countPrimeIn(num)
  printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, now - begin)

  def countPrimeIn(num: Int) = {
    val maxValidateNum = math.sqrt(num).toInt + 1
    val filter = new util.BitSet(num)

    filter.set(0, 2) // 0 and 1 are not prime

    @tailrec def setNonPrimeByProductOf(i: Int, j: Int): Option[_] = i * j match {
      case index if (index < num) => filter.set(index); setNonPrimeByProductOf(i, j + 1)
      case _                      => None
    }

    2 until maxValidateNum foreach { i => setNonPrimeByProductOf(i, i) }

    num - filter.cardinality()
  }

}
