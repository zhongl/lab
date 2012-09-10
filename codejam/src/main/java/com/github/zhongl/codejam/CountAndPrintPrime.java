package com.github.zhongl.codejam;

import java.util.BitSet;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class CountAndPrintPrime {
    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("Usage CMD <Number> <Count>");

        int number = Integer.parseInt(args[0]);
        int count = Integer.parseInt(args[1]);

        countAndPrintPrime(number, count);
    }

    private static void countAndPrintPrime(int number, int size) {
        Primes primes = new PrimeDetector(number).detect();
        size = primes.count() % 2 == 0 ? size * 2 : size * 2 - 1;
        String result = primes.outputMiddleOf(size);
        System.out.println(number + " " + size + ": " + result);
    }

}

class PrimeDetector {

    private final BitSet filter;
    private final int number;

    public PrimeDetector(int number) {
        this.number = number;
        this.filter = new BitSet(number);
    }

    public Primes detect() {
        filter.set(0); // 0 is not prime
        int sqrt = (int) Math.sqrt(number);
        for (int i = 2; i < sqrt + 1; i++) {
            for (int j = 2; ; j++) {
                int k = i * j;
                if (k > number) break;
                filter.set(k);
            }
        }
        return new Primes(filter, number - filter.cardinality() + 1);
    }
}

class Primes {

    private final BitSet filter;
    private final int count;

    Primes(BitSet filter, int count) {
        this.filter = filter;
        this.count = count;
    }

    public int count() { return count; }

    public String outputMiddleOf(int size) {
        if (count < size) return output(0, count);
        return output(count / 2 - (size / 2), size);
    }

    private String output(int begin, int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, index = 0; i < begin + size; i++, index++) {
            index = filter.nextClearBit(index);
            if (i < begin) continue;
            sb.append(index).append(" ");
        }
        return sb.toString();
    }
}
