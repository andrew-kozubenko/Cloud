package ru.nsu.cloud.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.nsu.cloud.api.RemoteTask;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class WorkerNodeTest {
    private WorkerNode worker;
    private ExecutorService executor;
    private final int TEST_PORT = 5001;

    @BeforeEach
    void setUp() {
        worker = new WorkerNode(TEST_PORT);
        executor = Executors.newSingleThreadExecutor();
        executor.execute(worker::start);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        executor.shutdownNow();
        Thread.sleep(500); // Даем время воркеру закрыться
    }

    @Test
    void testWorkerHandlesTaskCorrectly() throws Exception {
        // Создаем моковый RemoteTask
        RemoteTask<Integer, Integer> task = input -> input * 2;

        // Создаем сокет клиента
        try (Socket clientSocket = new Socket("localhost", TEST_PORT);
             ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream())) {

            // Отправляем задачу и данные
            oos.writeObject(task);
            oos.writeObject(5);
            oos.flush();

            // Читаем результат
            int result = (int) ois.readObject();
            assertEquals(10, result, "Worker должен корректно обработать задачу.");
        }
    }

    @Test
    void testWorkerHandlesMultipleTasks() throws Exception {
        RemoteTask<Integer, Integer> task = input -> input + 100;

        try (Socket client1 = new Socket("localhost", TEST_PORT);
             ObjectOutputStream oos1 = new ObjectOutputStream(client1.getOutputStream());
             ObjectInputStream ois1 = new ObjectInputStream(client1.getInputStream());

             Socket client2 = new Socket("localhost", TEST_PORT);
             ObjectOutputStream oos2 = new ObjectOutputStream(client2.getOutputStream());
             ObjectInputStream ois2 = new ObjectInputStream(client2.getInputStream())) {

            // Отправляем задачи параллельно
            oos1.writeObject(task);
            oos1.writeObject(1);
            oos1.flush();

            oos2.writeObject(task);
            oos2.writeObject(2);
            oos2.flush();

            // Проверяем результаты
            int result1 = (int) ois1.readObject();
            int result2 = (int) ois2.readObject();

            assertEquals(101, result1);
            assertEquals(102, result2);
        }
    }

    @Test
    void testWorkerHandlesTaskException() throws Exception {
        // Создаем задачу, которая выбрасывает исключение
        RemoteTask<Integer, Integer> task = input -> {
            throw new RuntimeException("Test Exception");
        };

        try (Socket clientSocket = new Socket("localhost", TEST_PORT);
             ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream())) {

            oos.writeObject(task);
            oos.writeObject(10);
            oos.flush();

            // Ожидаем, что воркер не упадет, но выбросит ошибку
            assertThrows(IOException.class, ois::readObject, "Worker должен корректно обработать исключение.");
        }
    }

    @Test
    void testWorkerHandlesShutdownCommand() throws Exception {
        // Создаем сокет клиента
        try (Socket clientSocket = new Socket("localhost", TEST_PORT);
             ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream())) {

            // Отправляем команду SHUTDOWN
            oos.writeObject("SHUTDOWN");
            oos.flush();

            // Проверяем, что воркер завершил свою работу
            assertFalse(worker.isRunning(), "Worker должен завершить свою работу после команды SHUTDOWN.");
        }
    }

//    @Test
//    void testWorkerClosesSocketAfterProcessing() throws Exception {
//        RemoteTask<Integer, Integer> task = input -> input * 3;
//
//        try (Socket clientSocket = new Socket("localhost", TEST_PORT);
//             ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
//             ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream())) {
//
//            oos.writeObject(task);
//            oos.writeObject(4);
//            oos.flush();
//
//            int result = (int) ois.readObject();
//            assertEquals(12, result);
//
//            // Проверяем, что сокет закрывается после обработки
//            assertTrue(clientSocket.isClosed(), "Сокет должен быть закрыт после обработки задачи.");
//        }
//    }
}
