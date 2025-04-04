package ru.nsu.cloud.worker;

import org.junit.jupiter.api.*;
import ru.nsu.cloud.api.LambdaTask;
import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.api.SerializableFunction;
import ru.nsu.cloud.master.Master;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerNodeTest {
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
    void testTaskExecutionWithList() throws Exception {
        // Создаем задачу с лямбдой, которая работает с List
        LambdaTask<String> task = new LambdaTask<>(
                (Object input) -> {
                    List<?> list = (List<?>) input;
                    return "Processed " + list.size() + " elements";
                },
                Arrays.asList("elem1", "elem2", "elem3")  // Вводим список
        );

        // Перехватываем консольный вывод
        ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStreamCaptor));

        // Отправляем задачу на выполнение мастеру
        master.submitTask(task);

        // Ждем выполнения задачи
        Thread.sleep(2000);

        // Проверяем, что воркер действительно выполнил задачу
        String consoleOutput = outputStreamCaptor.toString().trim();
        assertTrue(consoleOutput.contains("Processed 3 elements"));
    }

    @Test
    void testTaskExecutionWithoutInput() throws Exception {
        // Создаем задачу с лямбдой, которая не требует входных данных
        LambdaTask<String> task = new LambdaTask<>(
                (Object input) -> "No input provided",
                null  // Вводим null
        );

        // Перехватываем консольный вывод
        ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStreamCaptor));

        // Отправляем задачу на выполнение мастеру
        master.submitTask(task);

        // Ждем выполнения задачи
        Thread.sleep(2000);

        // Проверяем, что воркер действительно выполнил задачу
        String consoleOutput = outputStreamCaptor.toString().trim();
        assertTrue(consoleOutput.contains("No input provided"));
    }

    @Test
    void testTaskExecutionWithMultiplication() throws Exception {
        // Создаем задачу с лямбдой, которая умножает все элементы списка
        LambdaTask<Integer> task = new LambdaTask<>(
                (Object input) -> {
                    List<Integer> list = (List<Integer>) input;
                    return list.stream().reduce(1, (a, b) -> a * b); // Умножаем все элементы списка
                },
                Arrays.asList(1, 2, 3, 4)  // Вводим список чисел
        );

        // Перехватываем консольный вывод
        ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStreamCaptor));

        // Отправляем задачу на выполнение мастеру
        master.submitTask(task);

        // Ждем выполнения задачи
        Thread.sleep(2000);

        // Проверяем, что воркер действительно выполнил задачу
        String consoleOutput = outputStreamCaptor.toString().trim();
        assertTrue(consoleOutput.contains("Lambda executed, result: 24"));  // 1 * 2 * 3 * 4 = 24
    }
}
