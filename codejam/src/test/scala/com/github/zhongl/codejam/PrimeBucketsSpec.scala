package com.github.zhongl.codejam

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import com.github.zhongl.codejam.ThreadBasedPrime2.PrimeBuckets

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
class PrimeBucketsSpec extends FunSpec with ShouldMatchers {
  describe("PrimeBuckets") {
    it("should append prime and get the queue") {
      val cache = new PrimeBuckets(10)
      0 to 10 foreach { cache.append(_) }
      val buckets = cache.queue()
      buckets.size() should be(2)

      buckets.poll().size() should be(1)
      buckets.poll().get(9) should be(9)

    }
  }

}
