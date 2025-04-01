package ru.nsu.cloud.client;

import ru.nsu.cloud.api.JarExecutionTask;
import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.master.Master;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CloudContext {
    private final Master master;

    public CloudContext(String masterHost, int masterPort) {
        this.master = new Master(masterPort);
        new Thread(master::start).start();
    }

    public void stop() throws IOException {
        this.master.stop();
    }

    public <T> CloudDataset<T> parallelize(List<T> data) {
        return new CloudDataset<>(master, data);
    }

    public Object submitJar(String jarPath, String className, String methodName) throws ExecutionException, InterruptedException {
        JarExecutionTask task = new JarExecutionTask(jarPath, className, methodName);
        return master.submitTask(task).get();
    }

    public Object submitTask(RemoteTask task) throws ExecutionException, InterruptedException {
        return master.submitTask(task).get();
    }
}
