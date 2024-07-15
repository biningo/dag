package com.hiwuyue.dag.plugins;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import org.junit.Assert;
import org.junit.Test;

public class JavaJarDagTaskTest {
    @Test
    public void test() throws Exception {
        {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            System.setOut(printStream);

            URL url = JavaJarDagTaskTest.class.getClassLoader().getResource("hello.jar");
            Assert.assertNotNull(url);
            JavaJarDagTask javaJarDagTask = new JavaJarDagTask(new URL[] {url}, "test.hello.App", new String[] {"aaa", "bbb"});
            javaJarDagTask.run();

            String output = outputStream.toString();
            Assert.assertEquals("hello,args=[aaa, bbb]\n", output);
        }
        {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            System.setOut(printStream);

            URL url = JavaJarDagTaskTest.class.getClassLoader().getResource("hello.jar");
            Assert.assertNotNull(url);
            JavaJarDagTask javaJarDagTask = new JavaJarDagTask(new URL[] {url}, "test.hello.App");
            javaJarDagTask.run();

            String output = outputStream.toString();
            Assert.assertEquals("hello,args=[]\n", output);
        }
    }
}
