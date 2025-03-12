package ru.nsu.cloud;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.master.MasterNode;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;

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

                        // Выполняем задачу и отправляем результат обратно
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
    void testSendTaskToWorker() throws InterruptedException {
        MasterNode master = new MasterNode();

        // Задача: прибавить 10 к числу
        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        // Отправка задачи на фейковый Worker
        Integer result = master.sendTaskToWorker(addTen, 5, "localhost", TEST_PORT);

        // Проверка, что результат корректный
        assertNotNull(result);
        assertEquals(15, result); // 5 + 10 = 15
    }

    @Test
    void testSubmitAndExecuteTasks() {
        MasterNode master = new MasterNode();

        // Создаем задачи
        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;
        RemoteTask<String, String> toUpperCase = String::toUpperCase;

        // Отправляем задачи в Master
        master.submitTask(addTen);
        master.submitTask(toUpperCase);

        // извлекаем и выполняем задачи
        @SuppressWarnings("unchecked")
        RemoteTask<Integer, Integer> task1 = (RemoteTask<Integer, Integer>) master.getNextTask();
        @SuppressWarnings("unchecked")
        RemoteTask<String, String> task2 = (RemoteTask<String, String>) master.getNextTask();

        // Проверка результатов
        assertEquals(15, task1.apply(5));
        assertEquals("HELLO", task2.apply("hello"));
    }

    @Test
    void testResultAddedToQueue() throws InterruptedException {
        MasterNode master = new MasterNode();
        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        master.sendTaskToWorker(addTen, 5, "localhost", TEST_PORT);

        // Забираем результат из очереди
        Object result = master.getNextResult();
        assertEquals(15, result); // Проверка, что результат попал в очередь
    }
}
