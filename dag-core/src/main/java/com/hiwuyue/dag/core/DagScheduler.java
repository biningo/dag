package com.hiwuyue.dag.core;

import com.hiwuyue.dag.core.exception.DagGraphValidationException;

public interface DagScheduler {
    void start() throws DagGraphValidationException;

    void stop();

    boolean isRunning();

    long getDagTotalCostTime();
}
