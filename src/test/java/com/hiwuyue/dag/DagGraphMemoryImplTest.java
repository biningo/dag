package com.hiwuyue.dag;

import org.junit.Assert;
import org.junit.Test;

public class DagGraphMemoryImplTest {
    @Test
    public void testValidateAcyclic() {
        DagNode node1 = new DagNode("node1", new ShellDagTask("echo -n node1"));
        DagNode node2 = new DagNode("node2", new ShellDagTask("echo -n node2"));
        DagNode node3 = new DagNode("node3", new ShellDagTask("echo -n node3"));
        DagNode node4 = new DagNode("node4", new ShellDagTask("echo -n node4"));
        DagNode node5 = new DagNode("node5", new ShellDagTask("echo -n node5"));
        DagNode node6 = new DagNode("node6", new ShellDagTask("echo -n node6"));

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

        Assert.assertTrue(dagGraph.validateAcyclic());

        DagGraph badDagGraph = new DagGraphMemoryImpl("g2");
        badDagGraph.addNode(node1);
        badDagGraph.addNode(node2);
        badDagGraph.addNode(node3);
        badDagGraph.addNode(node4);
        badDagGraph.addNode(node5);
        badDagGraph.addNode(node6);

        badDagGraph.addEdge(node1, node2);
        badDagGraph.addEdge(node2, node1);
        badDagGraph.addEdge(node2, node3);

        Assert.assertFalse(badDagGraph.validateAcyclic());

    }
}
