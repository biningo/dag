package com.hiwuyue.dag;

public interface DagScheduler {
    void start() throws DagGraphValidationException;

    void stop();

    boolean isRunning();

    long getDagTotalCostTime();
}
