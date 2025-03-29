package ru.nsu.cloud.worker;

import org.junit.jupiter.api.*;
import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.master.Master;
import ru.nsu.cloud.worker.WorkerNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MasterWorkerTest {
    private static final int TEST_PORT = 5001;
    private Master master;
    private WorkerNode worker;
    private ExecutorService executorService;

    @BeforeAll
    void setUp() {
        executorService = Executors.newFixedThreadPool(2);

        // Запускаем Мастера
        master = new Master(TEST_PORT);
        executorService.submit(master::start);

        // Запускаем Воркера
        worker = new WorkerNode("localhost", TEST_PORT);
        executorService.submit(worker::start);

        // Даем время воркеру подключиться к мастеру
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @AfterAll
    void tearDown() throws IOException {
        worker.stopWorker();
        master.stop();
        executorService.shutdown();
    }

    @Test
    void testTaskExecution() throws Exception {
        // Создаем тестовую задачу
        RemoteTask task = () -> System.out.println("Task executed!");

        // Перехватываем консольный вывод
        ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStreamCaptor));

        // Отправляем задачу
        master.submitTask(task);

        // Ждем выполнения задачи
        Thread.sleep(2000);

        // Проверяем, что воркер действительно выполнил задачу
        String consoleOutput = outputStreamCaptor.toString().trim();
        assertTrue(consoleOutput.contains("Task executed!"));
    }
}
