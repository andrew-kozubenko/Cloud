package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerHandler implements Runnable {
    private final Master master;
    private final Socket workerSocket;
    private final BlockingQueue<RemoteTask> taskQueue;
    private final ConcurrentHashMap<String, CompletableFuture<Object>> taskResults;
    private Integer workerThreads;  // Количество потоков у воркера
    private ExecutorService executor;
    private AtomicInteger availableSlots;

    public WorkerHandler(Master master,
                         Socket workerSocket,
                         BlockingQueue<RemoteTask> taskQueue,
                         ConcurrentHashMap<String, CompletableFuture<Object>> taskResults) {
        this.master = master;
        this.workerSocket = workerSocket;
        this.taskQueue = taskQueue;
        this.taskResults = taskResults;
    }

    @Override
    public void run() {
        try (ObjectOutputStream out = new ObjectOutputStream(workerSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(workerSocket.getInputStream())) {
            this.workerThreads = in.readInt();
            this.executor = Executors.newFixedThreadPool(workerThreads);
            this.availableSlots = new AtomicInteger(workerThreads);
            this.master.addWorkerCores(workerThreads);

            while (!Thread.currentThread().isInterrupted()) {
                while (availableSlots.get() > 0) {
                    RemoteTask task = taskQueue.take();

                    out.writeObject(task);
                    out.flush();
                    availableSlots.decrementAndGet();

                    executor.submit(() -> {
                        try {
                            Integer taskId = in.readInt();
                            Object result = in.readObject(); // Получаем результат

                            // Завершаем future и передаем результат
                            CompletableFuture<Object> future = taskResults.remove(taskId);
                            if (future != null) {
                                future.complete(result);
                            }

                            System.out.println("Результат от воркера: " + result);

                        } catch (IOException | ClassNotFoundException e) {
                            System.err.println("Ошибка в WorkerHandler: " + e.getMessage());
                        } finally {
                            availableSlots.incrementAndGet(); // ↑↑↑ Освобождаем поток!
                        }
                    });
                }
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("WorkerHandler error: " + e.getMessage());
        }
    }
}
