package com.github.zhongl.codejam

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import java.util

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
class PrimesSpec extends FunSpec with ShouldMatchers {

  describe("Primes") {
    it("should have 5 primes") {
      new Primes(null, 5).count() should be(5)
    }
    it("should output middle 3 of 5 primes") {
      val filter = new util.BitSet(10)
      filter.set(1)
      filter.set(2)
      filter.set(3)
      filter.set(7)
      filter.set(9)

      new Primes(filter, 5).outputMiddleOf(3) should be("2 3 7 ")
    }
    it("should output middle 4 of 6 primes") {
      val filter = new util.BitSet(10)
      filter.set(1)
      filter.set(2)
      filter.set(3)
      filter.set(7)
      filter.set(8)
      filter.set(9)

      new Primes(filter, 6).outputMiddleOf(4) should be("2 3 7 8 ")
    }
    it("should output all 3 primes but expect 3") {
      val filter = new util.BitSet(10)
      filter.set(1)
      filter.set(2)
      filter.set(3)

      new Primes(filter, 3).outputMiddleOf(3) should be("1 2 3 ")
    }
    it("should output all 3 primes but expect 5") {
      val filter = new util.BitSet(10)
      filter.set(1)
      filter.set(2)
      filter.set(3)

      new Primes(filter, 3).outputMiddleOf(5) should be("1 2 3 ")
    }

  }
}
