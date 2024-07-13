package com.hiwuyue.dag.core;

public interface DagScheduler {
    void start() throws DagGraphValidationException;

    void stop();

    boolean isRunning();

    long getDagTotalCostTime();
}
