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
    private final ExecutorService threadPool = Executors.newFixedThreadPool(4); // Обрабатываем задачи в 4 потоках
    private volatile boolean running = true;  // Управляем работой воркера

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
                    break; // Выходим из цикла, если сокет закрыт
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

    private void handleRequest(Socket socket) {
        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

            logger.info("Reading task from client...");

            // Читаем задачу
            @SuppressWarnings("unchecked")
            RemoteTask<Object, Object> task = (RemoteTask<Object, Object>) ois.readObject();
            Object input = ois.readObject();

            // Выполняем задачу
            logger.info("Executing task...");
            Object result = task.apply(input);

            // Отправляем результат обратно
            logger.info("Sending result back to client...");
            oos.writeObject(result);
            oos.flush();

        } catch (EOFException | SocketException e) {
            logger.warning("Master disconnected. Shutting down worker.");
            stopWorker();

        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error while processing request", e);
        } finally {
            try {
                socket.close();
                logger.info("Socket closed: " + socket.isClosed());
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error closing socket", e);
            }
        }
    }

    public void stopWorker() {
        running = false;
    }

    private void shutdownWorker() {
        logger.info("Shutting down worker node...");
        threadPool.shutdown();
    }

    public static void main(String[] args) {
        WorkerNode worker = new WorkerNode(5000); // Запускаем на порту 5000
        worker.start();
    }
}
