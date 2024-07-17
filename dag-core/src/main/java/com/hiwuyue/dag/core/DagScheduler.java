package com.hiwuyue.dag.core;

import com.hiwuyue.dag.core.exception.DagGraphValidationException;

public interface DagScheduler {
    void run() throws DagGraphValidationException;

    void shutdown();

    boolean isRunning();

    long getDagTotalCostTime();
}
