package com.hiwuyue.dag.plugins;

import com.hiwuyue.dag.core.DagTask;
import java.net.URL;
import java.net.URLClassLoader;

public class JavaJarDagTask implements DagTask {

    private final URL[] jarUrls;

    private final String mainClassName;

    private String[] mainArgs = new String[0];

    public JavaJarDagTask(URL[] jarUrls, String mainClassName) {
        this.jarUrls = jarUrls;
        this.mainClassName = mainClassName;
    }

    public JavaJarDagTask(URL[] jarUrls, String mainClassName, String[] mainArgs) {
        this.jarUrls = jarUrls;
        this.mainClassName = mainClassName;
        this.mainArgs = mainArgs;
    }

    @Override
    public void run() throws Exception {
        URLClassLoader classLoader = new URLClassLoader(jarUrls);
        Class<?> mainClass = classLoader.loadClass(mainClassName);
        mainClass.getMethod("main", String[].class).invoke(null, (Object) mainArgs);
        classLoader.close();
    }
}
