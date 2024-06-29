package com.hiwuyue.dag;

public interface DagScheduler {
    void schedule() throws DagGraphValidationException;
}
