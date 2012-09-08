package com.github.zhongl.codejam;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class ThreadBasedPrime1 {
    public static void main(String[] args) throws InterruptedException {
        int num = Integer.parseInt(args[0]);
        long begin = currentTimeMillis();
        int count = countPrimeFromOneTo(num);
        long end = currentTimeMillis();
        out.printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, end - begin);
    }

    private static int countPrimeFromOneTo(final int num) throws InterruptedException {
        final int SIZE = Runtime.getRuntime().availableProcessors() * 2;
        final AtomicInteger SEQ = new AtomicInteger(2);
        final AtomicInteger COUNT = new AtomicInteger(1);
        final CountDownLatch LATCH = new CountDownLatch(SIZE);
        ExecutorService service = Executors.newFixedThreadPool(SIZE);

        for (int i = 0; i < SIZE; i++)
            service.execute(new Runnable() {

                @Override
                public void run() {
                    for (int i = SEQ.incrementAndGet(); i <= num; i = SEQ.incrementAndGet()) {
                        if (i % 10000 == 0) out.printf("%1$tT -> %2$,d\n", currentTimeMillis(), i);
                        if (!isPrime(i)) continue;
                        COUNT.incrementAndGet();
                    }
                    LATCH.countDown();
                }

            });

        LATCH.await();
        service.shutdownNow();
        return COUNT.get();
    }

    private static boolean isPrime(int i) {
        for (int j = 2; j < i; j++) if (i % j == 0) return false;
        return true;
    }

}
