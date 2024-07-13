package com.hiwuyue.dag.core;

public class DagNode {
    private String name;

    private volatile DagNodeState state = DagNodeState.PENDING;

    private final DagTask task;

    private long startTime;

    private long finishTime;

    public DagNode(String name, DagTask task) {
        this.name = name;
        this.task = task;
    }

    @Override
    public boolean equals(Object outer) {
        if (outer == this) {
            return true;
        }
        if (!(outer instanceof DagNode)) {
            return false;
        }
        DagNode outerNode = (DagNode) outer;
        return this.name.equals(outerNode.name);
    }

    public synchronized void setState(DagNodeState state) {
        this.state = state;
    }

    public synchronized DagNodeState getState() {
        return this.state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DagTask getTask() {
        return task;
    }

    public void runTask() throws Exception {
        task.run();
    }

    public void start() {
        this.setState(DagNodeState.RUNNING);
        this.startTime = System.currentTimeMillis();
    }

    public void success() {
        this.setState(DagNodeState.SUCCESSFUL);
        this.finishTime = System.currentTimeMillis();
    }

    public void fail() {
        this.setState(DagNodeState.FAILED);
        this.finishTime = System.currentTimeMillis();
    }

    public void interrupt() {
        this.setState(DagNodeState.UNREACHABLE);
    }

    public boolean isFinished() {
        DagNodeState state = this.getState();
        return state == DagNodeState.SUCCESSFUL || state == DagNodeState.FAILED || state == DagNodeState.UNREACHABLE;
    }

    public boolean isInterrupted() {
        DagNodeState state = this.getState();
        return state == DagNodeState.FAILED || state == DagNodeState.UNREACHABLE;
    }

    public boolean isPending() {
        return this.getState() == DagNodeState.PENDING;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    @Override
    public String toString() {
        return this.name;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }
}
