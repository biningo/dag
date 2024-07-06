package com.hiwuyue.dag;

public class DagNode {
    private String name;

    private volatile DagNodeState state = DagNodeState.PENDING;

    private final DagTask task;

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
    }

    public void success() {
        this.setState(DagNodeState.SUCCESSFUL);
    }

    public void fail() {
        this.setState(DagNodeState.FAILED);
    }

    public boolean isFinished() {
        DagNodeState state = this.getState();
        return state == DagNodeState.SUCCESSFUL || state == DagNodeState.FAILED;
    }

    public boolean isPending() {
        return this.getState() == DagNodeState.PENDING;
    }
}
