package com.github.zhongl.store.benchmark;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StatisticsCollector extends Thread {
    private final BlockingQueue<Message> mailbox = new LinkedBlockingQueue<Message>();
    private final Map<String, Statistics> statisticsMap = new HashMap<String, Statistics>();
    private volatile boolean running = true;

    @Override
    public void run() {
        while (running) {
            try {
                Message message = mailbox.poll(500, TimeUnit.MILLISECONDS);
                Statistics statistics = statisticsMap.get(message.operation);
                if (statistics == null) {
                    statistics = new Statistics(message.operation);
                    statisticsMap.put(message.operation, statistics);
                }
                if (message instanceof Elapse) {
                    statistics.addElapse(((Elapse) message).elapseMillis);
                }
                if (message instanceof Error) {
                    statistics.addError(((Error) message).t);
                }
            } catch (InterruptedException e) {
                // continue
            }
        }
    }

    public Collection<Statistics> haltAndGetStatistics() throws InterruptedException {
        running = false;
        join();
        return statisticsMap.values();
    }

    public void elapse(String operation, long elapseMillis) {
        mailbox.offer(new Elapse(operation, elapseMillis));
    }

    public void error(String opertion, Throwable t) {
        mailbox.offer(new Error(opertion, t));
    }

    private abstract static class Message {

        public final String operation;

        public Message(String operation) {
            this.operation = operation;
        }
    }

    private static class Error extends Message {

        public final Throwable t;

        public Error(String opertion, Throwable t) {
            super(opertion);
            this.t = t;
        }
    }

    private static class Elapse extends Message {

        public final long elapseMillis;

        public Elapse(String operation, long elapseMillis) {
            super(operation);
            this.elapseMillis = elapseMillis;
        }
    }
}
