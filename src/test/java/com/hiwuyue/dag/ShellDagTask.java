package com.hiwuyue.dag;

public class ShellDagTask implements DagTask {

    private final String shellScript;

    private String output;

    public ShellDagTask(String shellScript) {
        this.shellScript = shellScript;
    }

    @Override
    public void run() throws Exception {
        Process process = Runtime.getRuntime().exec(shellScript);
        output = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new Exception("shell script run failed! exitCode=" + exitCode);
        }
    }

    public String getOutput() {
        return this.output;
    }
}
