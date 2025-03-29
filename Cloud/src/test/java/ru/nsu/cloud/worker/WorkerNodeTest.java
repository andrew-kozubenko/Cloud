package ru.nsu.cloud.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerNodeTest {
    private static final int TEST_PORT = 5001;
    private ServerSocket serverSocket;
    private WorkerNode worker;
    private Future<?> workerFuture;

    @BeforeEach
    void setUp() throws IOException {
        serverSocket = new ServerSocket(TEST_PORT);

        worker = new WorkerNode("localhost", TEST_PORT);
        workerFuture = Executors.newSingleThreadExecutor().submit(worker::start);
    }

    @AfterEach
    void tearDown() throws IOException {
        worker.stopWorker();
        workerFuture.cancel(true);
        serverSocket.close();
    }

    @Test
    void testWorkerReceivesAndExecutesTask() throws Exception {
        try (Socket masterSocket = serverSocket.accept();
             ObjectOutputStream oos = new ObjectOutputStream(masterSocket.getOutputStream())) {

            // Создаем тестовую задачу
            RemoteTask task = new RemoteTask() {
                @Override
                public void execute() {
                    System.out.println("Task executed!");
                }
            };

            // Отправляем задачу воркеру
            oos.writeObject(task);
            oos.flush();

            // Захват вывода консоли, чтобы проверить, что воркер правильно выполнил задачу
            ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
            System.setOut(new PrintStream(outputStreamCaptor));

            // Ждем, пока воркер выполнит задачу
            Thread.sleep(1000);

            // Проверяем, что вывод содержит "Task executed!"
            String consoleOutput = outputStreamCaptor.toString().trim();
            assertTrue(consoleOutput.contains("Task executed!"));
        }
    }
}
