package com.github.zhongl.ipage;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Timer {
    private final long period;
    private long elpase;
    private long lastPoll;

    Timer(long period) {
        this.period = period;
    }

    public boolean poll() {
        if (lastPoll == 0) { // first poll
            lastPoll = System.nanoTime();
            return false;
        }
        long now = System.nanoTime();
        elpase += now - lastPoll;
        lastPoll = now;
        boolean timeout = elpase >= period;
        if (timeout) elpase = 0L; // reset elpase
        return timeout;
    }
}
