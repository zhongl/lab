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

  val (elapse, count) = time { countPrimeIn(num) }
  printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, elapse)

  def countPrimeIn(num: Int) = {
    val maxValidateNum = math.sqrt(num).toInt + 1
    val filter = new util.BitSet(num)

    filter.set(0) // 0 is not a prime
    filter.set(1) // 1 is not a prime

    @tailrec
    def setNonPrimeByProductOf(i: Int, j: Int) {
      i * j match {
        case index if (index <= num - 1) => filter.set(index); setNonPrimeByProductOf(i, j + 1)
        case _                           =>
      }
    }

    2 until maxValidateNum foreach { setNonPrimeByProductOf(_, 2) }

    filter.length() - filter.cardinality()
  }

  private def time[T](fun: => T) = {
    val begin = now
    val res = fun
    (now - begin, res)
  }

}
