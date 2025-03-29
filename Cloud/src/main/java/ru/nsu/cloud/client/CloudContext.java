package ru.nsu.cloud.client;

import ru.nsu.cloud.api.JarExecutionTask;
import ru.nsu.cloud.master.Master;
import java.util.List;

public class CloudContext {
    private final Master master;

    public CloudContext(String masterHost, int masterPort) {
        this.master = new Master(masterPort);
        new Thread(master::start).start();
    }

    public <T> CloudDataset<T> parallelize(List<T> data) {
        return new CloudDataset<>(master, data);
    }

    public void submitJar(String jarPath, String className, String methodName) {
        JarExecutionTask task = new JarExecutionTask(jarPath, className, methodName);
        master.submitTask(task);
    }
}
