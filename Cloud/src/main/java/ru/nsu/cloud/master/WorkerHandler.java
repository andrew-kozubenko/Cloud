package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class WorkerHandler implements Runnable {
    private static final Logger logger = Logger.getLogger(WorkerHandler.class.getName());

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

            logger.info("Connected to worker with " + workerThreads + " threads.");

            while (!Thread.currentThread().isInterrupted()) {
                while (availableSlots.get() > 0) {
                    RemoteTask task = taskQueue.take();
                    logger.info("Sending task " + task.getId() + " to worker.");

                    out.writeObject(task);
                    out.flush();
                    availableSlots.decrementAndGet();

                    executor.submit(() -> {
                        try {
                            Integer taskId;
                            Object result;

                            // Гарантируем атомарное считывание данных
                            synchronized (in) {
                                taskId = in.readInt();
                                result = in.readObject(); // Получаем результат
                            }

                            logger.info("Received result for task " + taskId + ": " + result);

                            // Завершаем future и передаем результат
                            CompletableFuture<Object> future = taskResults.remove(taskId);
                            if (future != null) {
                                future.complete(result);
                            }

                            System.out.println("Result from the worker: " + result);

                        } catch (IOException | ClassNotFoundException e) {
                            logger.severe("Error in WorkerHandler while receiving result: " + e.getMessage());
                        } finally {
                            availableSlots.incrementAndGet(); // ↑↑↑ Освобождаем поток!
                            logger.info("Worker slot freed. Available slots: " + availableSlots.get());
                        }
                    });
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.severe("WorkerHandler error: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
