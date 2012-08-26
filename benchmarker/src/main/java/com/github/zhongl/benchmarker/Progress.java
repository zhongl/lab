package com.github.zhongl.benchmarker;

import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Progress {

    public static final String FORMAT = "{0,number,#%}";
    private final CountDownLatch latch;
    private final int times;
    private final PrintStream out;
    private final int period;

    public Progress(int times, PrintStream out, int period) {
        this.times = times;
        this.out = out;
        this.period = period;
        latch = new CountDownLatch(times);
    }

    public void awaitAndprintStatus() throws InterruptedException {
        out.print("progress: ");
        out.print(status(0));
        backspace(2);
        while (latch.getCount() > 0) {
            latch.await(period, TimeUnit.SECONDS);
            String status = status(1 - (latch.getCount() * 1.0 / times));
            out.print(status);
            backspace(status.length());
        }
        out.println(status(1));
    }

    private void backspace(int size) {
        char[] backspaces = new char[size];
        Arrays.fill(backspaces, '\b');
        out.print(new String(backspaces));
    }

    private String status(double value) {return MessageFormat.format(FORMAT, value);}

    public void countDown() {
        latch.countDown();
    }
}
