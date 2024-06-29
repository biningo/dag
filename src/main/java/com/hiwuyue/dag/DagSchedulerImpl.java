package com.hiwuyue.dag;

import com.hiwuyue.dag.utils.ThreadUtil;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class DagSchedulerImpl implements DagScheduler {

    private DagGraph dagGraph;

    public DagSchedulerImpl(DagGraph dagGraph) {
        this.dagGraph = dagGraph;
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
                if (node.finished()) {
                    finishedCount++;
                }
                if (node.started()) {
                    continue;
                }

                Set<DagNode> prevNodes = nodeDependencies.get(node);
                if (prevNodes == null || prevNodes.isEmpty()) {
                    readyNodes.add(node);
                    continue;
                }

                boolean prevNodesFinished = true;
                for (DagNode prevNode : prevNodes) {
                    if (!prevNode.finished()) {
                        prevNodesFinished = false;
                        break;
                    }
                }

                if (prevNodesFinished) {
                    readyNodes.add(node);
                }
            }

            for (DagNode readyNode : readyNodes) {
                readyNode.startTask();
            }

            if (readyNodes.isEmpty() && finishedCount == nodes.size()) {
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
}
