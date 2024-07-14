package com.hiwuyue.dag.core;

public class SimpleShellDagTask implements DagTask {

    private final String shellScript;

    private String output;

    public SimpleShellDagTask(String shellScript) {
        this.shellScript = shellScript;
    }

    @Override
    public void run() throws Exception {
        Process process = Runtime.getRuntime().exec(new String[] {"sh", "-c", shellScript});
        output = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            output = new String(process.getErrorStream().readAllBytes());
            throw new Exception(String.format("shell script run failed! error=%s,exitCode=%d", output, exitCode));
        }
    }

    public String getOutput() {
        return this.output;
    }
}
