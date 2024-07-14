package com.hiwuyue.dag.core;

import org.junit.Assert;
import org.junit.Test;

public class DagNodeTest {

    @Test
    public void testDagNodeEqual() {
        DagNode node1 = new DagNode("node", new SimpleShellDagTask("echo -n 'hello,world'"));
        DagNode node2 = new DagNode("node", new SimpleShellDagTask("echo -n 'hello,world'"));
        Assert.assertEquals(node1, node2);
    }

    @Test
    public void testDagNodeState() {
        DagNode node = new DagNode("node", new SimpleShellDagTask("echo -n 'hello,world'"));
        Assert.assertTrue(node.isPending());
        node.start();
        Assert.assertFalse(node.isPending());
        Assert.assertFalse(node.isFinished());
        node.success();
        Assert.assertTrue(node.isFinished());
        Assert.assertFalse(node.isPending());
        node.start();
        node.fail();
        Assert.assertTrue(node.isFinished());
        Assert.assertFalse(node.isPending());
    }

    @Test
    public void testDagNodeRunTask() {
        String output = "hello,world\n";
        SimpleShellDagTask dagTask = new SimpleShellDagTask("echo hello,world");
        DagNode node = new DagNode("node", dagTask);
        try {
            node.runTask();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(output, dagTask.getOutput());
    }
}
