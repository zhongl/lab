package com.github.zhongl.ipage;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Timer {
    private final long period;
    private long elpase;
    private long lastPoll;

    Timer(long period) {
        this.period = period;
    }

    public boolean timeout() {
        if (lastPoll == 0) { // first timeout
            lastPoll = System.nanoTime();
            return false;
        }
        long now = System.nanoTime();
        elpase += now - lastPoll;
        lastPoll = now;
        return elpase >= period;
    }

    public void reset() {
        elpase = 0L;
    }
}
