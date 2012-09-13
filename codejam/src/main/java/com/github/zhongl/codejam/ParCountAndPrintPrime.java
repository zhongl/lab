package com.github.zhongl.codejam;

import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class ParCountAndPrintPrime {
    private final int number;
    private final int count;
    private final int parallels;
    private final int threshold;

    public static void main(String[] args) throws Exception {
        long begin = currentTimeMillis();
        parCountAndPrintPrime(args).run();
        System.out.printf("time elapse: %1$,d ms\n", currentTimeMillis() - begin);
    }

    private static ParCountAndPrintPrime parCountAndPrintPrime(String[] args) {
        switch (args.length) {
            case 2:
                return new ParCountAndPrintPrime(parseInt(args[0]), parseInt(args[1]));
            case 3:
                return new ParCountAndPrintPrime(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]));
            case 4:
                return new ParCountAndPrintPrime(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]));
            default:
                throw new IllegalArgumentException("Usage CMD <Number> <Count> [parallels=core*2] [threshold=]");
        }
    }

    private ParCountAndPrintPrime(int number, int count) {
        this(number, count, Runtime.getRuntime().availableProcessors() * 2);
    }

    private ParCountAndPrintPrime(int number, int count, int parallels) {
        this(number, count, parallels, 1000000);
    }

    public ParCountAndPrintPrime(int number, int count, int parallels, int threshold) {
        this.number = number;
        this.count = count;
        this.parallels = parallels;
        this.threshold = threshold;
    }

    public void run() throws Exception {
        int step = number / parallels - 1;
        List<Integer> primes;
        if (step < threshold) {
            primes = new PrimeDetector(number).call();
        } else {
            ExecutorService service = Executors.newFixedThreadPool(parallels);
            List<Future<List<Integer>>> futures = new ArrayList<Future<List<Integer>>>(parallels);
            for (int i = 0; i < number; i += step) {
                futures.add(service.submit(new PrimeDetector(i, Math.min(i + step, number))));
            }

            primes = new ArrayList<Integer>();
            for (Future<List<Integer>> future : futures) primes.addAll(future.get());
            service.shutdown();
        }
        System.out.println(number + " " + count + ": " + outputMiddleOf(primes));
    }

    private List<Integer> outputMiddleOf(List<Integer> primes) {
        int s = primes.size();
        int c = outputCount(s);
        if (s <= c) return primes;
        int begin = s / 2 - (c / 2);
        return primes.subList(begin, begin + c);
    }

    private int outputCount(int primes) {return primes % 2 == 0 ? count * 2 : count * 2 - 1;}

    private static final class PrimeDetector implements Callable<List<Integer>> {

        private final int from;
        private final int to;
        private final FastBitSet filter;

        public PrimeDetector(int number) {
            this(0, number);
        }

        public PrimeDetector(int from, int to) {
            this.from = from;
            this.to = to;
            this.filter = new FastBitSet(to - from);
            if (from == 0) filter.fastSet(0); // 0 is not a prime
        }

        @Override
        public List<Integer> call() throws Exception {
            setNotPrimes();
            return findPrimes();
        }

        private void setNotPrimes() {
            for (int i = 2; i < to / 2; i++) {
                for (int j = from / i; ; j++) {
                    int k = i * j;
                    if (k > to - 1) break;
                    if (k < from || j < i) continue;
                    filter.fastSet(k - from);
                }
            }
        }

        private List<Integer> findPrimes() {
            int size = to - from - filter.cardinality();
            List<Integer> list = new ArrayList<Integer>(size);
            for (int i = 0, index = 0; i < size; i++, index++) {
                index = filter.nextClearBit(index);
                list.add(from + index);
            }
            return list;
        }
    }

    static class FastBitSet {
        /* Used to shift left or right for a partial word mask */
        private static final long WORD_MASK = 0xffffffffffffffffL;

        protected long[] bits;
        protected int wlen; // number of words (elements) used in the array

        public FastBitSet(long numBits) {
            bits = new long[bits2words(numBits)];
            wlen = bits.length;
        }

        public static int bits2words(long numBits) {
            return (int) (((numBits - 1) >>> 6) + 1);
        }

        public void fastSet(int index) {
            int wordNum = index >> 6; // div 64
            int bit = index & 0x3f; // mod 64
            long bitmask = 1L << bit;
            bits[wordNum] |= bitmask;
        }

        public int cardinality() {
            int sum = 0;
            for (int i = 0; i < wlen; i++)
                sum = Long.bitCount(bits[i]);
            return sum;
        }

        public int nextClearBit(int index) {
            int i = index >> 6;
            if (i >= wlen)
                return index;

            long word = ~bits[i] & (WORD_MASK << index);
            while (true) {
                if (word != 0)
                    return (i << 6) + Long.numberOfTrailingZeros(word);
                if (++i == wlen)
                    return wlen << 6;
                word = ~bits[i];
            }
        }
    }
}
