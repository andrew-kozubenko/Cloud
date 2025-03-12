package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class MasterNode {
    private final BlockingQueue<RemoteTask<?, ?>> taskQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Object> resultQueue = new LinkedBlockingQueue<>();
    private final Map<String, Consumer<Object>> callbacks = new ConcurrentHashMap<>();

    public String submitTask(RemoteTask<?, ?> task) {
        String taskId = UUID.randomUUID().toString();
        try {
            taskQueue.put(task);
            System.out.println("Task added to queue: " + taskId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Failed to add task to queue: " + e.getMessage());
        }
        return taskId;
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

    public <T, R> void sendTaskToWorker(RemoteTask<T, R> task, T inputData, String workerHost, int workerPort, String taskId) {
        new Thread(() -> {
            try (Socket socket = new Socket(workerHost, workerPort);
                 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

                // Отправка задачи и данных
                oos.writeObject(task);
                oos.writeObject(inputData);
                oos.flush();

                // Получение результата
                R result = (R) ois.readObject();
                System.out.println("Received result from Worker: " + result);

                // Сохранение результата
                resultQueue.put(result);

                // Выполнение колбэка
                Consumer<Object> callback = callbacks.remove(taskId);
                if (callback != null) {
                    callback.accept(result);
                }

            } catch (IOException | ClassNotFoundException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void registerCallback(String taskId, Consumer<Object> callback) {
        callbacks.put(taskId, callback);
    }
}
