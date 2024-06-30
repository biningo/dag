package com.hiwuyue.dag;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hiwuyue.dag.utils.ThreadUtil;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DagSchedulerImpl implements DagScheduler {

    private DagGraph dagGraph;

    private ThreadPoolExecutor executor;

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
    public void schedule() throws DagGraphValidationException {
        if (!dagGraph.validateAcyclic()) {
            throw new DagGraphValidationException();
        }

        Set<DagNode> nodes = dagGraph.getDagNodes();
        Map<DagNode, Set<DagNode>> nodeDependencies = dagGraph.getDagNodeDependencies();

        while (true) {
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

            for (DagNode readyNode : readyNodes) {
                readyNode.start();
                this.executor.execute(new DagNodeRunner(readyNode));
            }

            if (finishedCount == nodes.size()) {
                break;
            }
            ThreadUtil.sleepIgnoreInterrupt(10);
        }
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
}
