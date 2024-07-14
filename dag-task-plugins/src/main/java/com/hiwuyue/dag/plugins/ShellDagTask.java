package com.hiwuyue.dag.plugins;

import com.hiwuyue.dag.core.DagTask;
import com.hiwuyue.dag.core.exception.DagTaskRunException;
import com.hiwuyue.dag.core.exception.DagTaskTimeoutException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ShellDagTask implements DagTask {

    private final String shellScript;

    private String output;

    private final int retry;

    private final long retryIntervalMs;

    private int actualRetry = 0;

    private final long timeoutMs;

    public ShellDagTask(Builder builder) {
        this.shellScript = builder.shellScript;
        this.retry = builder.retry;
        this.retryIntervalMs = builder.retryIntervalMs;
        this.timeoutMs = builder.timeoutMs;
    }

    @Override
    public void run() throws Exception {
        Exception finalErr = null;
        for (int i = 0; i < retry; i++) {
            this.actualRetry++;
            try {
                runShellScript();
                return;
            } catch (IOException | InterruptedException | DagTaskTimeoutException | DagTaskRunException e) {
                finalErr = e;
            }
            Thread.sleep(retryIntervalMs);
        }
        if (finalErr != null) {
            throw finalErr;
        }
    }

    private void runShellScript() throws IOException, InterruptedException, DagTaskTimeoutException, DagTaskRunException {
        Process process = Runtime.getRuntime().exec(new String[] {"sh", "-c", shellScript});
        if (timeoutMs > 0) {
            if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                process.destroy();
                throw new DagTaskTimeoutException();
            }
        } else {
            process.waitFor();
        }
        int exitCode = process.exitValue();
        if (exitCode != 0) {
            String errorOutput = new String(process.getErrorStream().readAllBytes());
            throw new DagTaskRunException(String.format("shell script run failed! error=%s,exitCode=%d", errorOutput, exitCode));
        }
        output = new String(process.getInputStream().readAllBytes());
    }

    public String getShellScript() {
        return shellScript;
    }

    public int getActualRetry() {
        return actualRetry;
    }

    public String getOutput() {
        return output;
    }

    public static Builder createBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String shellScript;

        private int retry = 1;

        private long retryIntervalMs = 0;

        private long timeoutMs = 0;

        public Builder shellScript(String shellScript) {
            this.shellScript = shellScript;
            return this;
        }

        public Builder timeout(long time, TimeUnit unit) {
            this.timeoutMs = unit.toMillis(time);
            return this;
        }

        public Builder retry(int retry) {
            this.retry = retry;
            return this;
        }

        public Builder retryInterval(long time, TimeUnit unit) {
            this.retryIntervalMs = unit.toMillis(time);
            return this;
        }

        public ShellDagTask build() {
            return new ShellDagTask(this);
        }
    }
}
