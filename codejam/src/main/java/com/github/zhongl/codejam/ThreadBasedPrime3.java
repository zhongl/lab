package com.github.zhongl.codejam;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class ThreadBasedPrime3 {

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
            default:
                return new Counter(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        }
    }

    static class Counter implements Callable<Integer>, PrimeListener {

        private final int end;
        private final int parallelSize;
        private final AtomicInteger count;
        private final ExecutorService service;

        Counter(int end) { this(end, 2); }

        Counter(int end, int factor) {
            this.end = end;
            count = new AtomicInteger(0);
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
            CountDownLatch latch = new CountDownLatch(parallelSize);

            EventBus bus = new EventBus();
            bus.register(this);

            List<Worker> workers = createWorkersBy(latch, bus);

            bus.onPrime(2);

            for (Worker worker : workers) service.execute(worker);

            latch.await();
            service.shutdown();

            return count.get();
        }

        private List<Worker> createWorkersBy(CountDownLatch latch, EventBus bus) {
            List<Worker> workers = new ArrayList<Worker>(parallelSize);
            for (Range range : Range.split(3, end, parallelSize))
                workers.add(new Worker(range, latch, bus));
            return workers;
        }

        @Override
        public void onPrime(int value) {
            count.incrementAndGet();
        }
    }

    interface PrimeListener {
        void onPrime(int value);
    }

    static class Worker implements Runnable, PrimeListener {

        private final Range range;
        private final CountDownLatch latch;
        private final EventBus bus;
        private final BlockingQueue<Integer> incomings;

        private volatile int maxPrime = 0;

        public Worker(Range range, CountDownLatch latch, EventBus bus) {
            this.range = range;
            this.latch = latch;
            this.bus = bus;
            incomings = new LinkedBlockingQueue<Integer>();
            bus.register(this);
        }

        @Override
        public void run() {
            ArrayList<Integer> primes = new ArrayList<Integer>();
            for (int i = range.from; i < range.to; i++) {
                incomings.drainTo(primes);
                if (checkPrime(i, primes)) bus.onPrime(i);
            }
            out.printf("%1$s is over.\n", range);
            latch.countDown();
        }

        private boolean checkPrime(int i, ArrayList<Integer> primes) {
            for (Integer prime : primes) if (i % prime == 0) return false;
            for (int j = maxPrime + 1; j < i; j++) if (i % j == 0) return false; // cover the missing primes.
            return true;
        }

        @Override
        public void onPrime(int value) {
            if (value >= range.to) return;
            maxPrime = Math.max(maxPrime, value);
            try { incomings.put(value); } catch (InterruptedException ignored) { }
        }
    }

    static class EventBus implements PrimeListener {
        private final Collection<PrimeListener> primeListeners = new CopyOnWriteArrayList<PrimeListener>();

        public void onPrime(int value) {
            for (PrimeListener primeListener : primeListeners) primeListener.onPrime(value);
        }

        public void register(PrimeListener primeListener) {
            primeListeners.add(primeListener);
        }

    }

    static class Range {
        final int from;
        final int to;

        Range(int from, int to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Range range = (Range) o;
            return from == range.from && to == range.to;
        }

        @Override
        public int hashCode() { return 31 * from + to; }

        @Override
        public String toString() { return "Range{from=" + from + ", to=" + to + '}'; }

        static List<Range> split(int from, int to, int parts) {
            int all = to - from;
            int step = all / (parts - 1);
            List<Range> ranges = new ArrayList<Range>();
            for (int i = 0; i < parts; i++) {
                int f = from + (i * step);
                int t = Math.min(f + step, to);
                ranges.add(new Range(f, t));
            }
            return ranges;
        }
    }

}
