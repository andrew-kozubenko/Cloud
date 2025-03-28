package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class WorkerHandler implements Runnable {
    private final Socket workerSocket;
    private final BlockingQueue<RemoteTask> taskQueue;

    public WorkerHandler(Socket workerSocket, BlockingQueue<RemoteTask> taskQueue) {
        this.workerSocket = workerSocket;
        this.taskQueue = taskQueue;
    }

    @Override
    public void run() {
        try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {

            while (!Thread.currentThread().isInterrupted()) {
                // Получаем новую задачу из очереди
                RemoteTask task = taskQueue.take();

                // Отправляем задачу воркеру
                out.writeObject(task);
                out.flush();

                // Получаем результат выполнения от воркера
                Object result = in.readObject();

                // Логируем результат (можно отправить его обратно в MasterNode)
                System.out.println("Результат от воркера: " + result);
            }
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            System.err.println("Ошибка в WorkerHandler: " + e.getMessage());
        }
    }
}
