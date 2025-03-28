package ru.nsu.cloud.client;

import ru.nsu.cloud.master.MasterNode;

import java.util.List;

public class CloudContext {
    private final MasterNode masterNode;

    public CloudContext(String masterHost, int masterPort) {
        this.masterNode = new MasterNode();
        this.masterNode.registerWorker(masterHost, masterPort);
    }

    public <T> CloudDataset<T> parallelize(List<T> data) {
        return new CloudDataset<>(masterNode, data);
    }
}
