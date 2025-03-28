package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

public class Master {
    private final int port;
    private ServerSocket serverSocket;
    private final BlockingQueue<RemoteTask> taskQueue = new LinkedBlockingQueue<>();
    private final ExecutorService workerPool = Executors.newCachedThreadPool();

    public Master(int port) {
        this.port = port;
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Master запущен на порту: " + port);

            while (!serverSocket.isClosed()) {
                Socket workerSocket = serverSocket.accept();
                System.out.println("Новый воркер подключен: " + workerSocket.getInetAddress());

                workerPool.submit(new WorkerHandler(workerSocket, taskQueue));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() throws IOException {
        serverSocket.close();
        workerPool.shutdown();
    }
}




//package ru.nsu.cloud.master;
//
//import ru.nsu.cloud.api.RemoteTask;
//import ru.nsu.cloud.api.SerializableFunction;
//
//import java.io.*;
//import java.net.Socket;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.function.Consumer;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//import java.util.stream.Stream;
//
//public class Master {
//    private final Map<String, RemoteTask<?, ?>> taskMap = new ConcurrentHashMap<>();
//    private final BlockingQueue<String> taskQueue = new LinkedBlockingQueue<>();
//    private final BlockingQueue<Object> resultQueue = new LinkedBlockingQueue<>();
//    private final Map<String, Consumer<Object>> callbacks = new ConcurrentHashMap<>();
//    private final List<WorkerNodeInfo> workers = new CopyOnWriteArrayList<>();
//    private final AtomicInteger currentWorkerIndex = new AtomicInteger(0);
//    private final AtomicInteger taskCount = new AtomicInteger(0);
//    int cores = Runtime.getRuntime().availableProcessors();
//    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(cores);
//    private final Map<WorkerNodeInfo, Socket> workerSockets = new ConcurrentHashMap<>();
//
//    public void connectToWorker(WorkerNodeInfo worker) {
//        try {
//            if (workerSockets.containsKey(worker) && !workerSockets.get(worker).isClosed()) {
//                System.out.println("Already connected to worker: " + worker);
//                return;
//            }
//
//            Socket socket = new Socket(worker.getHost(), worker.getPort());
//            workerSockets.put(worker, socket);
//            System.out.println("Connected to worker: " + worker);
//        } catch (IOException e) {
//            System.err.println("Failed to connect to worker: " + worker);
//        }
//    }
//
//
//    public String submitTask(RemoteTask<?, ?> task) {
//        String taskId = UUID.randomUUID().toString();
//        taskMap.put(taskId, task);
//        taskQueue.add(taskId); // Добавляем ID задачи в очередь
//        return taskId;
//    }
//
//    public RemoteTask<?, ?> getNextTask() {
//        try {
//            String taskId = taskQueue.take(); // Ждём, пока появится задача
//            return taskMap.remove(taskId);   // Удаляем задачу из taskMap
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            System.err.println("Failed to retrieve task from queue: " + e.getMessage());
//            return null;
//        }
//    }
//
//
//    public Object getNextResult() {
//        try {
//            return resultQueue.take();
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            System.err.println("Failed to retrieve result from queue: " + e.getMessage());
//            return null;
//        }
//    }
//
//    public <T, R> void sendTaskToWorker(RemoteTask<T, R> task,
//                                        List<SerializableFunction<?, ?>> dependencies,
//                                        List<T> inputBatch,
//                                        String taskId) {
//        WorkerNodeInfo worker = selectWorker();
//        if (worker == null) {
//            System.err.println("No available workers!");
//            return;
//        }
//
//        taskExecutor.submit(() -> {
//            try {
//                Socket workerSocket = workerSockets.get(worker);
//                if (workerSocket == null || workerSocket.isClosed()) {
//                    System.err.println("Worker socket closed! Trying to reconnect...");
//                    connectToWorker(worker);
//                    workerSocket = workerSockets.get(worker);
//                }
//
//                // Объявляем socket как final
//                final Socket socket = workerSocket;
//                if (socket == null) {
//                    System.err.println("Failed to connect to worker!");
//                    return;
//                }
//
//                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//                oos.writeObject(task);         // Отправляем саму задачу
//                oos.writeObject(dependencies); // Отправляем зависимости (функции)
//                oos.writeObject(inputBatch);   // Отправляем список входных данных (batch)
//                oos.flush();
//
//                taskExecutor.submit(() -> {
//                    try {
//                        readWorkerResponse(socket, taskId); // Теперь socket не изменяется
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                });
//
//            } catch (IOException e) {
//                e.printStackTrace();
//                worker.setAvailable(false);
//            }
//        });
//    }
//
//
//
//    private void readWorkerResponse(Socket socket, String taskId) {
//        try {
//            System.out.println("Reading result from worker...");
//            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
//            Object result = ois.readObject();
//            System.out.println("Received result from Worker: " + result);
//
//            resultQueue.put(result);
//
//            System.out.println("Invoking callback for task " + taskId + " with result: " + result);
//            Consumer<Object> callback = callbacks.remove(taskId);
//            if (callback != null) {
//                callback.accept(result);
//            }
//        } catch (IOException | ClassNotFoundException | InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void startHeartbeat() {
//        ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);
//        heartbeatScheduler.scheduleAtFixedRate(() -> {
//            for (WorkerNodeInfo worker : workers) {
//                try {
//                    Socket socket = workerSockets.get(worker);
//                    if (socket == null || socket.isClosed()) {
//                        System.err.println("Worker " + worker + " is unreachable! Reconnecting...");
//                        connectToWorker(worker);
//                    } else {
//                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//                        oos.writeObject("HEARTBEAT");
//                        oos.flush();
//                    }
//                } catch (IOException e) {
//                    worker.setAvailable(false);
//                    System.err.println("Lost connection to worker " + worker);
//                }
//            }
//        }, 0, 5, TimeUnit.SECONDS);
//    }
//
//
//    private WorkerNodeInfo selectWorker() {
//        return workers.stream()
//                .filter(WorkerNodeInfo::isAvailable)
//                .sorted(Comparator.comparingInt(WorkerNodeInfo::getLoad)
//                        .thenComparing(w -> currentWorkerIndex.getAndUpdate(i -> (i + 1) % workers.size())))
//                .findFirst()
//                .orElse(null);
//    }
//
//
//    public void updateWorkerLoad(String host, int port, int load) {
//        for (WorkerNodeInfo worker : workers) {
//            if (worker.getHost().equals(host) && worker.getPort() == port) {
//                worker.setLoad(load);
//                System.out.println("Updated load for Worker " + host + ":" + port + " -> " + load);
//                return;
//            }
//        }
//    }
//
//    public void registerCallback(String taskId, Consumer<Object> callback) {
//        System.out.println("Callback is registered for the issue: " + taskId);
//        callbacks.put(taskId, callback);
//    }
//
//    public void registerWorker(String host, int port) {
//        WorkerNodeInfo worker = new WorkerNodeInfo(host, port);
//        workers.add(worker);
//
//        connectToWorker(worker); // Устанавливаем постоянное соединение
//        System.out.println("Worker registered: " + host + ":" + port);
//    }
//
//
//    public <T, R> List<R> distributedMap(List<T> inputs, RemoteTask<T, R> task, List<SerializableFunction<?, ?>> dependencies) throws InterruptedException {
//        Map<String, List<Integer>> taskIndexMap = new HashMap<>(); // Связываем taskId с индексами входов
//        Map<String, CompletableFuture<List<R>>> taskFutureMap = new HashMap<>(); // Future для batch-результатов
//
//        int batchSize = 5; // Можно настроить размер batch
//        for (int i = 0; i < inputs.size(); i += batchSize) {
//            List<T> batch = inputs.subList(i, Math.min(i + batchSize, inputs.size()));
//            String taskId = submitTask(task);
//            taskIndexMap.put(taskId, IntStream.range(i, i + batch.size()).boxed().collect(Collectors.toList()));
//
//            CompletableFuture<List<R>> future = new CompletableFuture<>();
//            taskFutureMap.put(taskId, future);
//            registerCallback(taskId, result -> future.complete((List<R>) result));
//
//            sendTaskToWorker(task, dependencies, batch, taskId); // Отправляем batch
//        }
//
//        // Ожидание результатов
//        List<R> results = new ArrayList<>(Collections.nCopies(inputs.size(), null));
//
//        for (Map.Entry<String, List<Integer>> entry : taskIndexMap.entrySet()) {
//            String taskId = entry.getKey();
//            List<Integer> indices = entry.getValue();
//            CompletableFuture<List<R>> future = taskFutureMap.get(taskId);
//
//            try {
//                List<R> batchResults = future.get(2, TimeUnit.SECONDS); // Ждем batch-ответ
//                for (int j = 0; j < indices.size(); j++) {
//                    results.set(indices.get(j), batchResults.get(j));
//                }
//            } catch (ExecutionException | TimeoutException e) {
//                System.err.println("Task completion error: " + e.getMessage());
//            }
//        }
//
//        return results;
//    }
//
//    public <T, R> Stream<R> distributedStreamMap(Stream<T> data, RemoteTask<T, R> task, List<SerializableFunction<?, ?>> dependencies) {
//        Iterator<T> iterator = data.iterator();
//        BlockingQueue<R> resultQueue = new LinkedBlockingQueue<>();
//        AtomicInteger activeTasks = new AtomicInteger(0);
//        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
//
//        int batchSize = 10; // Размер батча, можно настроить
//        List<T> batch = new ArrayList<>(batchSize);
//
//        new Thread(() -> {
//            while (iterator.hasNext()) {
//                batch.add(iterator.next());
//
//                // Отправляем батч при достижении batchSize или если больше нет элементов
//                if (batch.size() == batchSize || !iterator.hasNext()) {
//                    String taskId = submitTask(task);
//                    activeTasks.incrementAndGet();
//
//                    registerCallback(taskId, result -> {
//                        try {
//                            synchronized (resultQueue) {
//                                ((List<R>) result).forEach(resultQueue::offer); // Добавляем весь результат
//                            }
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        } finally {
//                            if (activeTasks.decrementAndGet() == 0) {
//                                completionFuture.complete(null);
//                            }
//                        }
//                    });
//
//                    sendTaskToWorker(task, dependencies, new ArrayList<>(batch), taskId); // Передаем копию батча
//                    batch.clear(); // Очищаем для следующего набора
//                }
//            }
//            if (activeTasks.get() == 0) {
//                completionFuture.complete(null);
//            }
//        }).start();
//
//        return Stream.generate(() -> {
//            try {
//                return resultQueue.poll(10, TimeUnit.SECONDS); // Ограничиваем ожидание
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                return null;
//            }
//        }).takeWhile(Objects::nonNull).onClose(() -> {
//            try {
//                completionFuture.get(); // Ждём, пока все задачи завершатся
//            } catch (InterruptedException | ExecutionException e) {
//                Thread.currentThread().interrupt();
//            }
//        });
//    }
//
//
//
//    public void shutdownWorkers() {
//        Iterator<WorkerNodeInfo> iterator = workers.iterator();
//        while (iterator.hasNext()) {
//            WorkerNodeInfo worker = iterator.next();
//            try {
//                Socket socket = workerSockets.get(worker);
//                if (socket != null && !socket.isClosed()) {
//                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//                    oos.writeObject("SHUTDOWN");
//                    oos.flush();
//                    socket.close();
//                }
//                System.out.println("Worker " + worker + " has been shut down.");
//            } catch (IOException e) {
//                System.err.println("Failed to shut down worker " + worker);
//            } finally {
//                iterator.remove(); // Удаляем воркера из списка
//            }
//        }
//
//        // Даем время завершиться серверному потоку
//        try {
//            Thread.sleep(500);
//        } catch (InterruptedException ignored) {}
//    }
//
//
//
//}
