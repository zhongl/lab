package com.github.zhongl.codejam

import java.util
import annotation.tailrec
import System.{currentTimeMillis => now}
import actors.Actor._

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
object CountPrimeByParFilter extends App {
  val num = args match {
    case Array(n) => n.toInt
    case _        => throw new IllegalArgumentException("Usage: CMD <num>")
  }

  val (elapse, count) = time { countPrimeIn(num) }
  printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, elapse)


  def countPrimeIn(num: Int) = {
    val maxValidateNum = math.sqrt(num).toInt + 1
    val parallels = sys.runtime.availableProcessors() * 2
    val main = self

    0 until parallels foreach {
      n => actor {
        val filter = new util.BitSet(num)
        filter.set(0) // 0 is not a prime
        filter.set(1) // 1 is not a prime

        @tailrec
        def setNonPrimeByProductOf(i: Int, j: Int) {
          i * j match {
            case index if (index < num) => filter.set(index); setNonPrimeByProductOf(i, j + 1)
            case _                      =>
          }
        }
        (2 + n) until maxValidateNum by parallels foreach { i => setNonPrimeByProductOf(i, i) }
        main ! filter
      }

    }

    val filter: util.BitSet = new util.BitSet(num)
    var running = true
    var finished = 0
    while (running) {
      receive { case f: util.BitSet => filter or f; finished += 1; if (finished == parallels) running = false }
    }

    num - filter.cardinality()
  }

  private def time[T](fun: => T) = {
    val begin = now
    val res = fun
    (now - begin, res)
  }

}
