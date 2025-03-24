package ru.nsu.cloud.worker;

import ru.nsu.cloud.api.RemoteTask;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerNode {
    private static final Logger logger = Logger.getLogger(WorkerNode.class.getName());
    private final int port;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(4);
    private volatile boolean running = true;

    public WorkerNode(int port) {
        this.port = port;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            logger.info("WorkerNode started on port " + port);

            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    logger.info("Accepted connection from " + socket.getInetAddress());
                    threadPool.execute(() -> handleRequest(socket));
                } catch (SocketException e) {
                    logger.warning("ServerSocket closed, stopping worker...");
                    break;
                } catch (IOException e) {
                    if (running) {
                        logger.log(Level.WARNING, "Error accepting connection", e);
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Server encountered an error", e);
        } finally {
            shutdownWorker();
        }
    }

    private <T, R> void handleRequest(Socket socket) {
        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

            logger.info("Reading task from client...");

            while (running) {
                Object received;
                try {
                    received = ois.readObject();
                } catch (EOFException | SocketException e) {
                    logger.warning("Master disconnected. Shutting down worker.");
                    stopWorker();
                    break;
                }

                if ("SHUTDOWN".equals(received)) {
                    logger.info("Received SHUTDOWN command. Exiting...");
                    stopWorker();
                    break;
                }

                if (received instanceof RemoteTask<?, ?> rawTask) {
                    @SuppressWarnings("unchecked")
                    RemoteTask<T, R> task = (RemoteTask<T, R>) rawTask;

                    @SuppressWarnings("unchecked")
                    T input = (T) ois.readObject();

                    logger.info("Executing task...");
                    R result = task.apply(input);

                    logger.info("Sending result back to client...");
                    oos.writeObject(result);
                    oos.flush();
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error while processing request", e);
        } finally {
            try {
                socket.close();
                logger.info("Socket closed.");
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error closing socket", e);
            }
        }
    }

    public void stopWorker() {
        running = false;
        shutdownWorker();
    }

    private void shutdownWorker() {
        logger.info("Shutting down worker node...");
        threadPool.shutdown();
    }

    public boolean isRunning() {
        return running;
    }

    public static void main(String[] args) {
        WorkerNode worker = new WorkerNode(5000);
        worker.start();
    }
}
