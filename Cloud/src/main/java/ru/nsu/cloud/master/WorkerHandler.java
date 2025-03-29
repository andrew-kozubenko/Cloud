package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerHandler implements Runnable {
    private final Socket workerSocket;
    private final BlockingQueue<RemoteTask> taskQueue;
    private final ConcurrentHashMap<String, CompletableFuture<Object>> taskResults;

    public WorkerHandler(Socket workerSocket, BlockingQueue<RemoteTask> taskQueue, ConcurrentHashMap<String, CompletableFuture<Object>> taskResults) {
        this.workerSocket = workerSocket;
        this.taskQueue = taskQueue;
        this.taskResults = taskResults;
    }

    @Override
    public void run() {
        try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {

            while (!Thread.currentThread().isInterrupted()) {
                RemoteTask task = taskQueue.take();

                out.writeObject(task);
                out.flush();

                Object result = in.readObject();

                // Завершаем future и передаем результат
                CompletableFuture<Object> future = taskResults.remove(task.getId());
                if (future != null) {
                    future.complete(result);
                }

                System.out.println("Результат от воркера: " + result);
            }
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            System.err.println("Ошибка в WorkerHandler: " + e.getMessage());
        }
    }
}
