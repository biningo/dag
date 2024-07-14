package com.hiwuyue.dag.plugins;

import com.hiwuyue.dag.core.exception.DagTaskRunException;
import com.hiwuyue.dag.core.exception.DagTaskTimeoutException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class ShellDagTaskTest {

    @Test
    public void testShellDagTaskOutput() throws Exception {
        String exceptedOutput = "hello,world\n";
        ShellDagTask shellDagTask = ShellDagTask.createBuilder()
            .shellScript("echo 'hello,world'").build();
        shellDagTask.run();
        String output = shellDagTask.getOutput();
        Assert.assertEquals(exceptedOutput, output);
    }

    @Test
    public void testShellDagTaskTimeout() {
        {
            ShellDagTask shellDagTask = ShellDagTask.createBuilder()
                .shellScript("sleep 1").timeout(100, TimeUnit.MILLISECONDS)
                .build();
            try {
                shellDagTask.run();
                Assert.fail("No DagTaskTimeoutException");
            } catch (Exception e) {
                Assert.assertEquals(e.getClass().getSimpleName(), DagTaskTimeoutException.class.getSimpleName());
            }
        }

        {
            ShellDagTask shellDagTask = ShellDagTask.createBuilder()
                .shellScript("sleep 1").timeout(2000, TimeUnit.MILLISECONDS)
                .build();
            try {
                shellDagTask.run();
            } catch (Exception e) {
                Assert.fail("DagTaskTimeoutException thrown");
            }
        }

        {
            ShellDagTask shellDagTask = ShellDagTask.createBuilder()
                .shellScript("sleep 1").timeout(1000, TimeUnit.MILLISECONDS)
                .build();
            try {
                shellDagTask.run();
                Assert.fail("No DagTaskTimeoutException");
            } catch (Exception e) {
                Assert.assertEquals(e.getClass().getSimpleName(), DagTaskTimeoutException.class.getSimpleName());
            }
        }

        {
            ShellDagTask shellDagTask = ShellDagTask.createBuilder()
                .shellScript("sleep 1")
                .build();
            try {
                shellDagTask.run();
            } catch (Exception e) {
                Assert.fail("DagTaskTimeoutException thrown");
            }
        }
    }

    @Test
    public void testShellDagTaskRetry() throws Exception {
        {
            ShellDagTask shellDagTask = ShellDagTask.createBuilder()
                .shellScript("echo hello")
                .build();
            shellDagTask.run();
            Assert.assertEquals(1, shellDagTask.getActualRetry());
        }
        {
            int retryIntervalMs = 500;
            int retry = 2;
            ShellDagTask shellDagTask = ShellDagTask.createBuilder()
                .shellScript("aaa")
                .retry(retry)
                .retryInterval(retryIntervalMs, TimeUnit.MILLISECONDS)
                .build();
            long start = System.currentTimeMillis();
            try {
                shellDagTask.run();
                Assert.fail("No DagTaskRunException");
            } catch (DagTaskRunException err) {
                Assert.assertEquals(2, shellDagTask.getActualRetry());
                long retryCostMs = System.currentTimeMillis() - start;
                Assert.assertTrue((retryCostMs - retry * retryIntervalMs) < 50);
            }
        }
    }

}
