package com.github.zhongl.store.benchmark;

import java.text.MessageFormat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Statistics {

    private final String operation;
    private volatile int doneCount;
    private volatile int errorCount;
    private volatile long minElapse;
    private volatile long maxElapse;
    private volatile long totalElapse;

    public Statistics(String operation) {this.operation = operation;}

    public void addElapse(long elapseMillis) {
        totalElapse += elapseMillis;
        doneCount++;
        minElapse = Math.min(minElapse, elapseMillis);
        maxElapse = Math.max(maxElapse, elapseMillis);
    }

    @Override
    public String toString() {
        return MessageFormat.format("{0}\t: {1}/{2}={3}, ({4}, {5}), error<{6}>",
                operation,
                totalElapse,
                doneCount,
                (totalElapse / doneCount),
                minElapse,
                maxElapse,
                errorCount);
    }

    public void addError(Throwable t) {
        errorCount++;
    }
}
