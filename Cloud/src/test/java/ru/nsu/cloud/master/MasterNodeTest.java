package ru.nsu.cloud.master;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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
        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        AtomicReference<Object> callbackResult = new AtomicReference<>();
        String taskId = master.submitTask(addTen);

        master.registerCallback(taskId, callbackResult::set);
        master.sendTaskToWorker(addTen, 5, "localhost", TEST_PORT, taskId);

        // Ждем результат с колбэка
        Thread.sleep(100);

        assertEquals(15, callbackResult.get());
    }

    @Test
    void testSubmitAndRetrieveTasks() {
        MasterNode master = new MasterNode();

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
        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        String taskId = master.submitTask(addTen);
        master.sendTaskToWorker(addTen, 5, "localhost", TEST_PORT, taskId);

        // Ждем и проверяем, что результат попал в очередь
        Object result = master.getNextResult();
        assertEquals(15, result);
    }

    @Test
    void testCallbackExecution() throws InterruptedException {
        MasterNode master = new MasterNode();
        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        AtomicReference<Object> callbackResult = new AtomicReference<>();
        String taskId = master.submitTask(addTen);
        master.registerCallback(taskId, result -> {
            System.out.println("Callback received result: " + result);
            callbackResult.set(result);
        });

        master.sendTaskToWorker(addTen, 5, "localhost", TEST_PORT, taskId);

        // Ожидание для обработки колбэка
        Thread.sleep(100);

        assertEquals(15, callbackResult.get());
    }
}
