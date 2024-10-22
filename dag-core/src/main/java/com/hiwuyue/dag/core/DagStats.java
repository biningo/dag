package com.hiwuyue.dag.core;

import java.util.Map;
import java.util.Set;

public class DagStats {

    private final DagGraph dagGraph;

    private long dagTotalCostTime;

    private long nodeTotalCostTime;

    private long nodeAvgCostTime;

    private long nodeMaxCostTime;

    private long nodeMinCostTime;

    private int successCount;

    private int failCount;

    private int runningCount;

    private int pendingCount;

    private int unreachableCount;

    private int nodeCount;

    private int dagMaxConcurrency;

    public DagStats(DagGraph dagGraph) {
        this.dagGraph = dagGraph;
    }

    public void compute() {
        Set<DagNode> nodes = dagGraph.getDagNodes();
        this.nodeCount = nodes.size();
        for (DagNode node : nodes) {
            switch (node.getState()) {
                case SUCCESSFUL:
                    this.successCount++;
                    break;
                case FAILED:
                    this.failCount++;
                    break;
                case PENDING:
                    this.pendingCount++;
                    break;
                case RUNNING:
                    this.runningCount++;
                    break;
                case UNREACHABLE:
                    this.unreachableCount++;
                    break;
            }

            long costTime = node.getFinishTime() - node.getStartTime();
            this.nodeTotalCostTime += costTime;
            if (this.nodeMaxCostTime < costTime) {
                this.nodeMaxCostTime = costTime;
            }
            if (this.nodeMinCostTime > costTime) {
                this.nodeMinCostTime = costTime;
            }
        }
        this.nodeAvgCostTime = this.nodeTotalCostTime / this.nodeCount;

        Map<DagNode, Set<DagNode>> nodeDependencies = dagGraph.getDagNodeDependencies();
        nodeDependencies.forEach((node, prevNodes) -> {
            if (this.dagMaxConcurrency < prevNodes.size()) {
                this.dagMaxConcurrency = prevNodes.size();
            }
        });
    }

    public DagGraph getDagGraph() {
        return dagGraph;
    }

    public long getDagTotalCostTime() {
        return dagTotalCostTime;
    }

    public void setDagTotalCostTime(long dagTotalCostTime) {
        this.dagTotalCostTime = dagTotalCostTime;
    }

    public long getNodeAvgCostTime() {
        return nodeAvgCostTime;
    }

    public long getNodeMaxCostTime() {
        return nodeMaxCostTime;
    }

    public long getNodeMinCostTime() {
        return nodeMinCostTime;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public int getFailCount() {
        return failCount;
    }

    public int getRunningCount() {
        return runningCount;
    }

    public int getPendingCount() {
        return pendingCount;
    }

    public int getUnreachableCount() {
        return unreachableCount;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getDagMaxConcurrency() {
        return dagMaxConcurrency;
    }

    public long getNodeTotalCostTime() {
        return nodeTotalCostTime;
    }
}
