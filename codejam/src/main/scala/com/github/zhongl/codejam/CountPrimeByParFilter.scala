package com.github.zhongl.codejam

import java.util
import annotation.tailrec
import System.{currentTimeMillis => now}
import util.concurrent.atomic.AtomicInteger
import util.concurrent.CountDownLatch

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
    val latch = new CountDownLatch(parallels)
    val corp = new AtomicInteger(2)

    val ts = 0 until parallels map {
      i =>

        val t = new Thread {
          val filter = new util.BitSet(num)

          filter.set(0) // 0 is not a prime
          filter.set(1)

          // 1 is not a prime

          @tailrec
          def setNonPrimeByProductOf(i: Int, j: Int) {
            i * j match {
              case index if (index < num) => filter.set(index); setNonPrimeByProductOf(i, j + 1)
              case _                      =>
            }
          }

          var n = corp.getAndIncrement
          while (n < maxValidateNum) {
            setNonPrimeByProductOf(n, n)
            n = corp.getAndIncrement
          }

          latch.countDown()
        }

        t.start()
        t
    }

    latch.await()

    val filter = ts.foldLeft(new util.BitSet()) { (f, t) => f.or(t.filter); f }

    num - filter.cardinality()
  }

  private def time[T](fun: => T) = {
    val begin = now
    val res = fun
    (now - begin, res)
  }

}
