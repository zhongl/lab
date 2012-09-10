package com.github.zhongl.codejam

import collection.mutable.ArrayBuffer
import System.{currentTimeMillis => now}

object CountPrimeByDivision extends App {

  val num = args match {
    case Array(n) => n.toInt
    case _        => throw new IllegalArgumentException
  }

  val (elapse, count) = time { countPrimeIn(num) }
  printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, elapse)

  private def countPrimeIn(num: Int) = {
    val primes = ArrayBuffer(2) // 2 is the first num number

    @inline def isPrime(n: Int) = (primes /*.par*/ find { n % _ == 0 }).isEmpty

    3 to num foreach { n => if (isPrime(n)) primes += n }
    primes.size
  }

  private def time[T](fun: => T) = {
    val begin = now
    val res = fun
    (now - begin, res)
  }
}

