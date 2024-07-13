package com.hiwuyue.dag.core.utils;

import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutorService;
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

    public static void shutdownExecutor(ExecutorService executor, int timeout, TimeUnit unit) {
        Preconditions.checkNotNull(executor);
        executor.shutdown();
        try {
            executor.awaitTermination(timeout, unit);
        } catch (InterruptedException ignored) {
        }
    }
}
