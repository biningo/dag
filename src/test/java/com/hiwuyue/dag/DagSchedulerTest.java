package com.hiwuyue.dag;

import org.junit.Assert;
import org.junit.Test;

public class DagSchedulerTest {
    @Test
    public void testNodeStats() throws DagGraphValidationException {
        DagNode node1 = new DagNode("node1", new ShellDagTask("sleep 1 && echo -n node1"));
        DagNode node2 = new DagNode("node2", new ShellDagTask("sleep 1 && echo -n node2"));
        DagNode node3 = new DagNode("node3", new ShellDagTask("sleep 2 && echo -n node3"));
        DagNode node4 = new DagNode("node4", new ShellDagTask("echo -n node4"));
        DagNode node5 = new DagNode("node5", new ShellDagTask("echo -n node5"));
        DagNode node6 = new DagNode("node6", new ShellDagTask("sleep 1 && echo -n node6"));

        DagGraph dagGraph = new DagGraphMemoryImpl("g1");
        dagGraph.addNode(node1);
        dagGraph.addNode(node2);
        dagGraph.addNode(node3);
        dagGraph.addNode(node4);
        dagGraph.addNode(node5);
        dagGraph.addNode(node6);

        dagGraph.addEdge(node1, node2);
        dagGraph.addEdge(node1, node3);
        dagGraph.addEdge(node2, node4);
        dagGraph.addEdge(node3, node4);
        dagGraph.addEdge(node4, node5);

        DagScheduler scheduler = new DagSchedulerImpl(dagGraph);
        scheduler.start();

        DagStats dagStats = new DagStats(dagGraph);
        dagStats.compute();
        dagStats.setDagTotalCostTime(scheduler.getDagTotalCostTime());

        Assert.assertEquals(6, dagStats.getNodeCount());
        Assert.assertEquals(6, dagStats.getSuccessCount());
        Assert.assertEquals(0, dagStats.getFailCount());

        Assert.assertEquals(2, dagStats.getDagMaxConcurrency());

        long nodeTotalCostTime = (1 + 1 + 2 + 1) * 1000;
        Assert.assertEquals(nodeTotalCostTime / 1000, dagStats.getNodeTotalCostTime() / 1000);
        Assert.assertEquals(1 + 2, dagStats.getDagTotalCostTime() / 1000);
        Assert.assertEquals(0, dagStats.getNodeMinCostTime());
        Assert.assertEquals(2, dagStats.getNodeMaxCostTime() / 1000);
        Assert.assertTrue((dagStats.getNodeAvgCostTime() - nodeTotalCostTime / 6) < 1000);
    }
}
