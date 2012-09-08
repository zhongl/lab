package com.github.zhongl.codejam

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import ThreadBasedPrime3.Range

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
class RangeSpec extends FunSpec with ShouldMatchers {
  describe("Range") {
    it("should split to ranges") {
      val ranges = Range.split(3, 17, 4)
      ranges.size() should be(4)
      ranges.get(0) should be(new Range(3, 7))
      ranges.get(1) should be(new Range(7, 11))
      ranges.get(2) should be(new Range(11, 15))
      ranges.get(3) should be(new Range(15, 17))
    }
  }
}
