package ru.nsu.cloud.worker;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;


public class WorkerNode {
    private static final Logger logger = Logger.getLogger(WorkerNode.class.getName());
    private final String masterHost;
    private final int masterPort;
    private volatile boolean running = true;
    private final int coreCount;
    private final ExecutorService executorService;

    public WorkerNode(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.coreCount = Runtime.getRuntime().availableProcessors(); // Получаем количество ядер
        this.executorService = Executors.newFixedThreadPool(coreCount); // Создаем пул потоков
    }

    public void start() {
        while (running) {
            try (Socket socket = new Socket(masterHost, masterPort);
                 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

                logger.info("Connected to master at " + masterHost + ":" + masterPort);

                // Отправляем количество ядер мастеру
                oos.writeInt(coreCount);
                oos.flush();
                logger.info("Sent CPU core count: " + coreCount);

                while (running) {
                    try {
                        logger.info("reeding");
                        Object received = ois.readObject();
                        logger.info("end reeding");

                        if ("SHUTDOWN".equals(received)) {
                            logger.info("Received SHUTDOWN command. Exiting...");
                            stopWorker();
                            break;
                        }

                        if (received instanceof RemoteTask<?> task) {
                            logger.info("Received task: " + task.getId());

                            // Запускаем выполнение задачи в отдельном потоке
                            executorService.submit(() -> executeTask(task, oos));
                        }
                    } catch (EOFException | SocketException | ClassNotFoundException e) {
                        logger.warning("Master disconnected. Retrying in 5 seconds...");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                        break; // Выходим из внутреннего while и пробуем переподключиться
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
        logger.info("Worker finished.");
    }

    private <T> void executeTask(RemoteTask<T> task, ObjectOutputStream oos) {
        try {
            T result = task.execute(); // Выполняем задачу
            logger.info("Task " + task.getId() + " executed. Sending result...");

            synchronized (oos) { // Синхронизация отправки данных по сокету
                oos.writeObject(task.getId()); // Отправляем ID задачи
                oos.writeObject(result); // Отправляем результат
                oos.flush();
            }

            logger.info("Result for task " + task.getId() + " sent.");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error executing task " + task.getId(), e);
        }
    }

    public void stopWorker() {
        running = false;
        executorService.shutdown();
        logger.info("Worker shutting down...");
    }

    public static void main(String[] args) {
//        if (args.length < 2) {
//            System.out.println("Usage: java WorkerNode <master-host> <master-port>");
//            return;
//        }
//
//        String masterHost = args[0];
//        int masterPort = Integer.parseInt(args[1]);

        //WorkerNode worker = new WorkerNode(masterHost, masterPort);
        WorkerNode worker = new WorkerNode("192.168.84.203", 9090);
        worker.start();
    }
}

