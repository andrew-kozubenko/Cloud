package ru.nsu.cloud.master;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MasterNodeTest {
    private static final int TEST_PORT = 5000;

    @BeforeAll
    static void startMockWorker() {
        Executors.newSingleThreadExecutor().submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(TEST_PORT)) {
                while (true) {
                    try (Socket socket = serverSocket.accept();
                         ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                         ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

                        // Получаем задачу и данные
                        @SuppressWarnings("unchecked")
                        RemoteTask<Integer, Integer> task = (RemoteTask<Integer, Integer>) ois.readObject();
                        Integer input = (Integer) ois.readObject();

                        // Выполняем задачу и отправляем результат
                        Integer result = task.apply(input);
                        oos.writeObject(result);
                        oos.flush();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    @Test
    void testSendTaskToWorkerWithCallback() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;
        AtomicReference<Object> callbackResult = new AtomicReference<>();

        String taskId = master.submitTask(addTen);
        master.registerCallback(taskId, callbackResult::set);

        master.sendTaskToWorker(addTen, 5, taskId);

        Thread.sleep(100); // Ждем обработку

        assertEquals(15, callbackResult.get());
    }


    @Test
    void testSubmitAndRetrieveTasks() {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;
        RemoteTask<String, String> toUpperCase = String::toUpperCase;

        master.submitTask(addTen);
        master.submitTask(toUpperCase);

        @SuppressWarnings("unchecked")
        RemoteTask<Integer, Integer> task1 = (RemoteTask<Integer, Integer>) master.getNextTask();
        @SuppressWarnings("unchecked")
        RemoteTask<String, String> task2 = (RemoteTask<String, String>) master.getNextTask();

        assertEquals(15, task1.apply(5));
        assertEquals("HELLO", task2.apply("hello"));
    }

    @Test
    void testResultAddedToQueue() {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        String taskId = master.submitTask(addTen);
        master.sendTaskToWorker(addTen, 5, taskId);

        // Ждем и проверяем, что результат попал в очередь
        Object result = master.getNextResult();
        assertEquals(15, result);
    }

    @Test
    void testCallbackExecution() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        AtomicReference<Object> callbackResult = new AtomicReference<>();
        String taskId = master.submitTask(addTen);
        master.registerCallback(taskId, result -> {
            System.out.println("Callback received result: " + result);
            callbackResult.set(result);
        });

        master.sendTaskToWorker(addTen, 5, taskId);

        // Ожидание для обработки колбэка
        Thread.sleep(100);

        assertEquals(15, callbackResult.get());
    }

    @Test
    void testDistributedMap() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> multiplyByTwo = (input) -> input * 2;

        List<Integer> inputs = List.of(1, 2, 3, 4, 5);
        List<Integer> results = master.distributedMap(inputs, multiplyByTwo);

        assertEquals(List.of(2, 4, 6, 8, 10), results);
    }

    @Test
    void testDistributedStreamMap() {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> square = (input) -> input * input;

        Stream<Integer> inputStream = Stream.of(2, 3, 4);
        Stream<Integer> resultStream = master.distributedStreamMap(inputStream, square);

        assertEquals(List.of(4, 9, 16), resultStream.toList());
    }

    @Test
    void testWorkerTimeoutHandling() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> slowTask = (input) -> {
            try {
                Thread.sleep(5000); // Задача "зависает" на 5 секунд
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return input * 2;
        };

        String taskId = master.submitTask(slowTask);
        master.sendTaskToWorker(slowTask, 5, taskId);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Object> future = executor.submit(master::getNextResult);

        try {
            Object result = future.get(2, TimeUnit.SECONDS);
            fail("Expected timeout, but got: " + result);
        } catch (TimeoutException e) {
            System.out.println("✅ TimeoutException caught as expected!");
        } catch (ExecutionException e) {
            System.err.println("❌ ExecutionException: " + e.getCause().getMessage());
            fail("Unexpected ExecutionException occurred!");
        } finally {
            executor.shutdown();
        }
    }

}
