package ru.nsu.cloud.client;

import ru.nsu.cloud.master.Master;
import java.util.List;

public class CloudContext {
    private final Master master;

    public CloudContext(String masterHost, int masterPort) {
        this.master = new Master(masterPort);
        new Thread(master::start).start();
        this.master.registerWorker(masterHost, masterPort);
    }

    public <T> CloudDataset<T> parallelize(List<T> data) {
        return new CloudDataset<>(master, data);
    }
}
