package ru.nsu.cloud.worker;

import ru.nsu.cloud.api.RemoteTask;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerNode {
    private final int port;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(4); // Обрабатываем задачи в 4 потоках

    public WorkerNode(int port) {
        this.port = port;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("WorkerNode started on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                threadPool.execute(() -> handleRequest(socket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleRequest(Socket socket) {
        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

            // Читаем задачу
            @SuppressWarnings("unchecked")
            RemoteTask<Object, Object> task = (RemoteTask<Object, Object>) ois.readObject();
            Object input = ois.readObject();

            // Выполняем задачу
            Object result = task.apply(input);

            // Отправляем результат обратно
            oos.writeObject(result);
            oos.flush();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    public static void main(String[] args) {
        WorkerNode worker = new WorkerNode(5000); // Запускаем на порту 5000
        worker.start();
    }
}
