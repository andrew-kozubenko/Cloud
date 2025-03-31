package ru.nsu.cloud.master;

import ru.nsu.cloud.api.LambdaTask;
import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.api.SerializableFunction;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class Master {
    private static final Logger logger = Logger.getLogger(Master.class.getName());

    private final int port;
    private ServerSocket serverSocket;
    private final BlockingQueue<RemoteTask> taskQueue = new LinkedBlockingQueue<>();
    private final ExecutorService workerPool = Executors.newCachedThreadPool();
    private final ConcurrentHashMap<String, CompletableFuture<Object>> taskResults = new ConcurrentHashMap<>();
    private final AtomicInteger workerCoresCount = new AtomicInteger(0);

    public void addWorkerCores(Integer cores) {
        workerCoresCount.addAndGet(cores);
        logger.info("Added worker cores. Total available cores: " + workerCoresCount.get());
    }

    public Master(int port) {
        this.port = port;
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Master started on port: " + port);

            while (!serverSocket.isClosed()) {
                Socket workerSocket = serverSocket.accept();
                logger.info("New worker connected from " + workerSocket.getInetAddress());

                workerPool.submit(new WorkerHandler(this, workerSocket, taskQueue, taskResults));
            }
        } catch (IOException e) {
            logger.severe("Error in Master: " + e.getMessage());
        }
    }

    public void stop() throws IOException {
        logger.info("Stopping Master...");
        serverSocket.close();
        workerPool.shutdown();
    }

    public Future<Object> submitTask(RemoteTask task) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        taskResults.put(task.getId(), future);  // Сохраняем future
        try {
            taskQueue.put(task);
            logger.info("Task added to queue: " + task.getId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.severe("Error when adding a task to the queue: " + e.getMessage());
            throw new RuntimeException("Error when adding a task to the queue", e);
        }
        return future; // Возвращаем Future, чтобы ожидать результат
    }

    public <T, R> Future<List<R>> remoteMap(SerializableFunction<T, R> function, List<T> data) {
        CompletableFuture<List<R>> future = new CompletableFuture<>();
        List<Future<Object>> taskFutures = new ArrayList<>();
        List<List<T>> batches = new ArrayList<>();

        int batchSize = (int) Math.ceil((double) data.size() / workerCoresCount.get()); // Размер батча
        logger.info("Splitting data into batches. Batch size: " + batchSize);

        // Разбиваем данные на батчи и сохраняем их порядок
        for (int i = 0; i < data.size(); i += batchSize) {
            List<T> batch = new ArrayList<>(data.subList(i, Math.min(i + batchSize, data.size())));
            batches.add(batch);
            logger.info("Created batch of size: " + batch.size());
        }

        // Приведение типа, чтобы соответствовало LambdaTask
        @SuppressWarnings("unchecked")
        SerializableFunction<Object, List<R>> adaptedFunction = (Object input) ->
                ((List<T>) input).stream().map(function).toList();

        // Создаем и отправляем задачи
        for (List<T> batch : batches) {
            LambdaTask<List<R>> task = new LambdaTask<>(adaptedFunction, batch);
            taskFutures.add(submitTask(task));
            logger.info("Submitted batch task with " + batch.size() + " elements.");
        }

        // Собираем результаты в правильном порядке
        Executors.newCachedThreadPool().submit(() -> {
            try {
                List<R> results = new ArrayList<>();
                for (Future<Object> taskFuture : taskFutures) {
                    List<R> batchResult = (List<R>) taskFuture.get();
                    results.addAll(batchResult);
                    logger.info("Received batch result of size: " + batchResult.size());
                }
                future.complete(results); // Заполняем CompletableFuture списком результатов
                logger.info("All tasks completed. Final result size: " + results.size());
            } catch (Exception e) {
                logger.severe("Error while collecting batch results: " + e.getMessage());
                future.completeExceptionally(e); // Если ошибка, передаем ее в future
            }
        });

        return future;
    }
}