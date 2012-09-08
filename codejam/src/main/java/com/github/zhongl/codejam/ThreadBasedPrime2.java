package com.github.zhongl.codejam;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class ThreadBasedPrime2 {

    public static void main(String[] args) throws Exception {
        int num = Integer.parseInt(args[0]);
        long begin = currentTimeMillis();
        int count = newCounter(args).call();
        long end = currentTimeMillis();
        out.printf("the count of primes from 1 to %1$s is: %2$s, time elapse: %3$,d ms\n", num, count, end - begin);
    }

    private static Counter newCounter(String[] args) {
        switch (args.length) {
            case 1:
                return new Counter(Integer.parseInt(args[0]));
            case 2:
                return new Counter(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
            case 3:
                return new Counter(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            default:
                return new Counter(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        }
    }

    static class PrimeBuckets {
        private final int bucketSize;
        private final ArrayList<Bucket> buckets;

        PrimeBuckets(int bucketSize) {
            this.bucketSize = bucketSize;
            this.buckets = new ArrayList<Bucket>();
            buckets.add(new Bucket(bucketSize));
        }

        public void append(int prime) {
            boolean append = buckets.get(0).append(prime);
            if (!append) {
                buckets.add(0, new Bucket(bucketSize));
                append(prime);
            }
        }

        public Queue<Bucket> queue() {
            return new ConcurrentLinkedQueue<Bucket>(buckets);
        }

        public int size() {
            int sum = 0;
            for (Bucket bucket : buckets) {
                sum += bucket.size();
            }
            return sum;
        }

        static class Bucket {

            private final int[] primes;

            private volatile int size;

            Bucket(int capacity) {
                primes = new int[capacity];
            }

            public boolean append(int prime) {
                if (size == primes.length) return false;
                primes[size++] = prime;
                return true;
            }

            public int get(int index) {
                return primes[index];
            }

            public int size() {
                return size;
            }

        }
    }

    static class Counter implements Callable<Integer> {

        private final int parallelSize;
        private final ExecutorService service;
        private final PrimeBuckets buckets;

        private final int end;
        private final int per;

        Counter(int end) { this(end, 10000); }

        Counter(int end, int per) { this(end, per, 10000);}

        Counter(int end, int per, int bucketSize) { this(end, per, bucketSize, 2); }

        Counter(int end, int per, int bucketSize, int factor) {
            this.end = end;
            this.per = per;
            buckets = new PrimeBuckets(bucketSize);
            buckets.append(2);
            parallelSize = Runtime.getRuntime().availableProcessors() * factor;
            service = Executors.newFixedThreadPool(parallelSize, new ThreadFactory() {
                private int i = 0;

                @Override
                public Thread newThread(Runnable runnable) {
                    return new Thread(runnable, "counter-worker-" + i++);
                }
            });
        }

        @Override
        public Integer call() throws Exception {
            for (int i = 3; i < end; i++) if (isPrime(i)) buckets.append(i);

            service.shutdown();
            return buckets.size() + 1;
        }

        private boolean isPrime(int i) {
            if (i % per == 0) System.out.println(new Date() + " -> " + i);
            return parrallelCheckPrime(i);
        }

        private boolean parrallelCheckPrime(int i) {
            final AtomicBoolean isPrime = new AtomicBoolean(true);
            final AtomicBoolean running = new AtomicBoolean(true);
            final CountDownLatch latch = new CountDownLatch(parallelSize);

            Queue<PrimeBuckets.Bucket> queue = buckets.queue();
            for (int j = 0; j < parallelSize; j++) {
                service.execute(new Worker(i, queue, running, isPrime, latch));
            }

            try { latch.await(); } catch (InterruptedException ignored) { }

            return isPrime.get();
        }

        class Worker implements Runnable {

            private final int num;
            private final Queue<PrimeBuckets.Bucket> buckets;
            private final AtomicBoolean running;
            private final AtomicBoolean isPrime;
            private final CountDownLatch latch;

            public Worker(int num, Queue<PrimeBuckets.Bucket> buckets, AtomicBoolean running, AtomicBoolean isPrime, CountDownLatch latch) {
                this.num = num;
                this.buckets = buckets;
                this.running = running;
                this.isPrime = isPrime;
                this.latch = latch;
            }

            @Override
            public void run() {
                for (; ; ) {
                    PrimeBuckets.Bucket bucket = buckets.poll();
                    if (bucket == null || isNumModPrimeZeroIn(bucket)) break;
                }

                latch.countDown();
            }

            private boolean isNumModPrimeZeroIn(PrimeBuckets.Bucket bucket) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (num % bucket.get(i) == 0 && running.compareAndSet(true, false)) {
                        isPrime.set(false);
                        return true;
                    }
                    if (!running.get()) return true;
                }
                return false;
            }
        }

    }

}
