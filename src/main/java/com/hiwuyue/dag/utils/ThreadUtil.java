package com.hiwuyue.dag.utils;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadUtil {
    public static void sleepIgnoreInterrupt(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }

    public static ThreadPoolExecutor multiThread(Runnable command, int num) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(num, num, 0, TimeUnit.SECONDS,
            new SynchronousQueue<>());
        for (int i = 0; i < num; i++) {
            executor.execute(command);
        }
        return executor;
    }

    public static ThreadPoolExecutor singleThread(Runnable runnable) {
        return multiThread(runnable, 1);
    }
}
