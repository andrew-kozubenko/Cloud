package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MasterNode {
    private final BlockingQueue<RemoteTask<?, ?>> taskQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Object> resultQueue = new LinkedBlockingQueue<>();

    public void submitTask(RemoteTask<?, ?> task) {
        try {
            taskQueue.put(task);
            System.out.println("Task added to queue: " + task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Failed to add task to queue: " + e.getMessage());
        }
    }

    public RemoteTask<?, ?> getNextTask() {
        try {
            return taskQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Failed to retrieve task from queue: " + e.getMessage());
            return null;
        }
    }

    public Object getNextResult() {
        try {
            return resultQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Failed to retrieve result from queue: " + e.getMessage());
            return null;
        }
    }

    /**
     * Отправляет задачу на Worker и получает результат
     */
    public <T, R> R sendTaskToWorker(RemoteTask<T, R> task, T inputData,
                                     String workerHost, int workerPort) throws InterruptedException {
        try (Socket socket = new Socket(workerHost, workerPort);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            // Отправляем задачу
            oos.writeObject(task);
            oos.writeObject(inputData);
            oos.flush();

            R result = (R) ois.readObject();
            System.out.println("Received result from Worker: " + result);
            resultQueue.put(result);
            return result;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}

