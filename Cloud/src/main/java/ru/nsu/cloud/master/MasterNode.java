package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class MasterNode {
    private final BlockingQueue<RemoteTask<?, ?>> taskQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Object> resultQueue = new LinkedBlockingQueue<>();
    private final Map<String, Consumer<Object>> callbacks = new ConcurrentHashMap<>();
    private final List<WorkerNodeInfo> workers = new CopyOnWriteArrayList<>();
    private final AtomicInteger currentWorkerIndex = new AtomicInteger(0);
    private final AtomicInteger taskCount = new AtomicInteger(0);

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

    public <T, R> void sendTaskToWorker(RemoteTask<T, R> task, T inputData, String taskId) {
        new Thread(() -> {
            WorkerNodeInfo worker = selectWorker();
            if (worker == null) {
                System.err.println("No available workers!");
                return;
            }

            try (Socket socket = new Socket(worker.getHost(), worker.getPort());
                 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

                // Отправляем задачу
                oos.writeObject(task);
                oos.writeObject(inputData);
                oos.flush();

                // Получаем результат
                R result = (R) ois.readObject();
                System.out.println("Received result from Worker: " + result);

                // Сохраняем результат
                resultQueue.put(result);

                // Вызываем колбэк
                Consumer<Object> callback = callbacks.remove(taskId);
                if (callback != null) {
                    callback.accept(result);
                }

            } catch (IOException | ClassNotFoundException | InterruptedException e) {
                e.printStackTrace();
                worker.setAvailable(false); // Помечаем Worker как недоступный
            }
        }).start();
    }

    private WorkerNodeInfo selectWorker() {
        return workers.stream()
                .filter(WorkerNodeInfo::isAvailable)
                .sorted(Comparator.comparingInt(WorkerNodeInfo::getLoad)
                        .thenComparing(w -> currentWorkerIndex.getAndUpdate(i -> (i + 1) % workers.size())))
                .findFirst()
                .orElse(null);
    }


    public void updateWorkerLoad(String host, int port, int load) {
        for (WorkerNodeInfo worker : workers) {
            if (worker.getHost().equals(host) && worker.getPort() == port) {
                worker.setLoad(load);
                System.out.println("Updated load for Worker " + host + ":" + port + " -> " + load);
                return;
            }
        }
    }

    public void registerCallback(String taskId, Consumer<Object> callback) {
        System.out.println("Колбэк зарегистрирован для задачи: " + taskId);
        callbacks.put(taskId, callback);
    }

    public void registerWorker(String host, int port) {
        workers.add(new WorkerNodeInfo(host, port));
        System.out.println("Worker registered: " + host + ":" + port);
    }

    public <T, R> List<R> distributedMap(List<T> inputs, RemoteTask<T, R> task) throws InterruptedException {
        List<CompletableFuture<R>> futures = new ArrayList<>();
        Map<String, Integer> taskIndexMap = new HashMap<>(); // Связываем taskId с индексом в списке
        Map<String, CompletableFuture<R>> taskFutureMap = new HashMap<>();


        for (int i = 0; i < inputs.size(); i++) {
            T input = inputs.get(i);
            String taskId = submitTask(task);
            taskIndexMap.put(taskId, i);

            CompletableFuture<R> future = new CompletableFuture<>();
            taskFutureMap.put(taskId, future); // Привязываем future к taskId
            registerCallback(taskId, result -> future.complete((R) result));

            sendTaskToWorker(task, input, taskId);
            futures.add(future);
        }

        // Ожидание результатов с обработкой исключений
        List<R> results = new ArrayList<>(Collections.nCopies(inputs.size(), null));

        for (Map.Entry<String, Integer> entry : taskIndexMap.entrySet()) {
            String taskId = entry.getKey();
            int index = entry.getValue();
            CompletableFuture<R> future = taskFutureMap.get(taskId); // Получаем future по taskId

            try {
                R result = future.get(2, TimeUnit.SECONDS); // Ожидание результата
                results.set(index, result);
            } catch (ExecutionException | TimeoutException e) {
                System.err.println("Ошибка выполнения задачи: " + e.getMessage());
            }
        }

        return results;
    }


    public <T, R> Stream<R> parallelStreamMap(List<T> data, RemoteTask<T, R> task) {
        try {
            List<R> results = distributedMap(data, task);
            return results.stream();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Stream.empty();
        }
    }

    public <T, R> Stream<R> distributedStreamMap(Stream<T> data, RemoteTask<T, R> task) {
        Iterator<T> iterator = data.iterator();
        BlockingQueue<R> resultQueue = new LinkedBlockingQueue<>();
        AtomicInteger activeTasks = new AtomicInteger(0); // Счётчик активных задач
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();

        new Thread(() -> {
            while (iterator.hasNext()) {
                T next = iterator.next();
                String taskId = submitTask(task);
                activeTasks.incrementAndGet();

                registerCallback(taskId, result -> {
                    try {
                        resultQueue.put((R) result);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        if (activeTasks.decrementAndGet() == 0) {
                            completionFuture.complete(null);
                        }
                    }
                });

                sendTaskToWorker(task, next, taskId);
            }
            if (activeTasks.get() == 0) {
                completionFuture.complete(null);
            }
        }).start();

        return Stream.generate(() -> {
            try {
                return resultQueue.poll(10, TimeUnit.SECONDS); // Ограничиваем ожидание
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }).takeWhile(Objects::nonNull).onClose(() -> {
            try {
                completionFuture.get(); // Ждём, пока все задачи завершатся
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
            }
        });
    }


}
