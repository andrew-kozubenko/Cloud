package ru.nsu.cloud.master;

import java.util.concurrent.atomic.AtomicInteger;

public class WorkerNodeInfo {
    private final String host;
    private final int port;
    private final AtomicInteger load = new AtomicInteger(0);
    private boolean available = true;

    public WorkerNodeInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getLoad() {
        return load.get();
    }

    public void setLoad(int newLoad) {
        load.set(newLoad);
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }
}

