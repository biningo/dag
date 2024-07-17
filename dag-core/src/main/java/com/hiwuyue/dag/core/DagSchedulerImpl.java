package com.hiwuyue.dag.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hiwuyue.dag.core.exception.DagGraphValidationException;
import com.hiwuyue.dag.core.utils.ThreadUtil;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DagSchedulerImpl implements DagScheduler {

    private DagGraph dagGraph;

    private ThreadPoolExecutor executor;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private long startTime;
    private long stopTime;

    public DagSchedulerImpl(DagGraph dagGraph) {
        this.dagGraph = dagGraph;
        setDefaultExecutor();
    }

    public DagSchedulerImpl(DagGraph dagGraph, ThreadPoolExecutor executor) {
        this.dagGraph = dagGraph;
        this.executor = executor;
    }

    private void setDefaultExecutor() {
        this.executor = new ThreadPoolExecutor(
            10, 100,
            0, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("dag-scheduler-%d").build(),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @Override
    public void run() throws DagGraphValidationException {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        if (!dagGraph.validateAcyclic()) {
            throw new DagGraphValidationException();
        }

        this.startTime = System.currentTimeMillis();

        Set<DagNode> nodes = dagGraph.getDagNodes();
        Map<DagNode, Set<DagNode>> nodeDependencies = dagGraph.getDagNodeDependencies();

        while (this.running.get()) {
            ArrayList<DagNode> readyNodes = new ArrayList<>();
            int finishedCount = 0;
            for (DagNode node : nodes) {
                if (node.isFinished()) {
                    finishedCount++;
                    continue;
                }
                if (!node.isPending()) {
                    continue;
                }

                Set<DagNode> prevNodes = nodeDependencies.get(node);
                if (prevNodes == null || prevNodes.isEmpty()) {
                    readyNodes.add(node);
                    continue;
                }

                boolean prevNodesFinished = true;
                for (DagNode prevNode : prevNodes) {
                    if (!prevNode.isFinished()) {
                        prevNodesFinished = false;
                        break;
                    }
                }

                if (prevNodesFinished) {
                    readyNodes.add(node);
                }
            }

            ArrayList<DagNode> waitingNodes = new ArrayList<>();
            for (DagNode readyNode : readyNodes) {
                Set<DagNode> prevNodes = nodeDependencies.get(readyNode);
                for (DagNode prevNode : prevNodes) {
                    if (prevNode.isInterrupted()) {
                        readyNode.interrupt();
                        break;
                    }
                }
                if (readyNode.getState() == DagNodeState.PENDING) {
                    waitingNodes.add(readyNode);
                }
            }

            for (DagNode waitingNode : waitingNodes) {
                waitingNode.start();
                this.executor.execute(new DagNodeRunner(waitingNode));
            }

            if (finishedCount == nodes.size()) {
                break;
            }

            if (readyNodes.isEmpty()) {
                ThreadUtil.sleepIgnoreInterrupt(10);
            }
        }
        this.running.set(false);
        this.stopTime = System.currentTimeMillis();
    }

    @Override
    public void shutdown() {
        if (!this.running.compareAndSet(true, false)) {
            return;
        }
        ThreadUtil.shutdownExecutor(executor, 3, TimeUnit.SECONDS);
        this.stopTime = System.currentTimeMillis();
    }

    public DagGraph getDagGraph() {
        return dagGraph;
    }

    public void setDagGraph(DagGraph dagGraph) {
        this.dagGraph = dagGraph;
    }

    private static class DagNodeRunner implements Runnable {
        private final DagNode node;

        public DagNodeRunner(DagNode node) {
            this.node = node;
        }

        @Override
        public void run() {
            try {
                node.runTask();
                node.success();
            } catch (Exception err) {
                node.fail();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public long getDagTotalCostTime() {
        return this.stopTime - this.startTime;
    }
}
