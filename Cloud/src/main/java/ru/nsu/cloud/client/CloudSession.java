package ru.nsu.cloud.client;

import java.io.IOException;

public class CloudSession {
    private final CloudContext cloudContext;

    private CloudSession(CloudContext cloudContext) {
        this.cloudContext = cloudContext;
    }

    public static CloudSessionBuilder builder() {
        return new CloudSessionBuilder();
    }

    public CloudContext cloudContext() {
        return cloudContext;
    }

    public void stop() throws IOException {
        this.cloudContext.stop();
    }

    public static class CloudSessionBuilder {
        private String masterHost = "localhost";
        private int masterPort = 9090;

        public CloudSessionBuilder master(String host, int port) {
            this.masterHost = host;
            this.masterPort = port;
            return this;
        }

        public CloudSession build() {
            CloudContext context = new CloudContext(masterHost, masterPort);
            return new CloudSession(context);
        }
    }
}
