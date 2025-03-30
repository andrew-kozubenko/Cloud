package ru.nsu.cloud.worker;

import ru.nsu.cloud.api.RemoteTask;
import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerNode {
    private static final Logger logger = Logger.getLogger(WorkerNode.class.getName());
    private final String masterHost;
    private final int masterPort;
    private volatile boolean running = true;

    public WorkerNode(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public void start() {
        while (running) {
            try (Socket socket = new Socket(masterHost, masterPort);
                 ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

                logger.info("Connected to master at " + masterHost + ":" + masterPort);

                while (running) {
                    try {
                        Object received = ois.readObject();

                        if ("SHUTDOWN".equals(received)) {
                            logger.info("Received SHUTDOWN command. Exiting...");
                            stopWorker();
                            break;
                        }

                        if (received instanceof RemoteTask task) {
                            logger.info("Executing task...");
                            Object result = task.execute();  // Выполняем задачу и получаем результат

                            // Отправляем результат выполнения задачи обратно мастеру
                            oos.writeObject(result);
                            oos.flush();
                            logger.info("Task executed, result sent back to master.");
                        }
                    } catch (EOFException | SocketException | ClassNotFoundException e) {
                        logger.warning("Master disconnected. Retrying in 5 seconds...");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                        break;  // Выходим из внутреннего while и пробуем переподключиться
                    }
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "Failed to connect to master. Retrying in 5 seconds...", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void stopWorker() {
        running = false;
        logger.info("Worker shutting down...");
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java WorkerNode <master-host> <master-port>");
            return;
        }

        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);

        WorkerNode worker = new WorkerNode(masterHost, masterPort);
        worker.start();
    }
}
