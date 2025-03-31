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

            while (!Thread.currentThread().isInterrupted() && !workerSocket.isClosed()) {
                try {
                    while (availableSlots.get() > 0) {
                        RemoteTask task = taskQueue.poll(500, TimeUnit.MILLISECONDS); // Ждём задачу с таймаутом
                        if (task == null) continue; // Если задачи нет, проверяем состояние

                        logger.info("Sending task " + task.getId() + " to worker.");
                        out.writeObject(task);
                        out.flush();
                        availableSlots.decrementAndGet();

                        executor.submit(() -> {
                            try {
                                Object taskId;
                                Object result;

                                synchronized (in) {
                                    taskId = in.readObject();
                                    result = in.readObject();
                                }
                                logger.info("Received result for task " + taskId + ": " + result);

                                CompletableFuture<Object> future = taskResults.remove(taskId);
                                if (future != null) {
                                    future.complete(result);
                                }

                            } catch (IOException | ClassNotFoundException e) {
                                logger.severe("Error receiving result: " + e.getMessage());
                                Thread.currentThread().interrupt(); // Останавливаем поток, если воркер отключился
                            } finally {
                                availableSlots.incrementAndGet();
                                logger.info("Worker slot freed. Available slots: " + availableSlots.get());
                            }
                        });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break; // Прерываем цикл при завершении
                }
            }
        } catch (IOException e) {
            logger.severe("WorkerHandler IO error: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    private void shutdown() {
        logger.info("Shutting down WorkerHandler...");
        try {
            workerSocket.close();
        } catch (IOException e) {
            logger.warning("Error closing socket: " + e.getMessage());
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
        logger.info("WorkerHandler shutdown complete.");
    }

}
