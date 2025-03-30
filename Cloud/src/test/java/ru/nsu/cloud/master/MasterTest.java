package ru.nsu.cloud.master;

import org.junit.jupiter.api.*;
import ru.nsu.cloud.api.LambdaTask;
import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.api.SerializableFunction;
import ru.nsu.cloud.client.CloudContext;
import ru.nsu.cloud.client.CloudDataset;
import ru.nsu.cloud.client.CloudSession;
import ru.nsu.cloud.master.Master;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MasterTest {
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
    void testMasterReceivesResultWithListProcessing() throws Exception {
        // Создаем задачу с лямбдой, которая считает количество элементов в списке
        LambdaTask<String, String> task = new LambdaTask<>(
                (List<String> input) -> "Processed " + input.size() + " elements",
                Arrays.asList("elem1", "elem2", "elem3")
        );

        // Отправляем задачу и получаем результат от мастера
        String result = master.submitTask(task);

        // Проверяем, что мастер получил правильный результат
        assertEquals("Processed 3 elements", result);
    }

    @Test
    void testMasterReceivesResultWithoutInput() throws Exception {
        // Создаем задачу, которая не требует входных данных
        LambdaTask<String, String> task = new LambdaTask<>(
                (List<String> input) -> "No input provided",
                null
        );

        // Отправляем задачу и получаем результат от мастера
        String result = master.submitTask(task);

        // Проверяем, что мастер получил правильный результат
        assertEquals("No input provided", result);
    }

    @Test
    void testMasterReceivesResultWithMultiplication() throws Exception {
        // Создаем задачу с умножением всех элементов списка на 2
        LambdaTask<List<Integer>, List<Integer>> task = new LambdaTask<>(
                (List<Integer> input) -> input.stream().map(x -> x * 2).toList(),
                Arrays.asList(1, 2, 3, 4, 5)
        );

        // Отправляем задачу и получаем результат от мастера
        List<Integer> result = master.submitTask(task);

        // Проверяем, что мастер получил правильный результат
        assertEquals(Arrays.asList(2, 4, 6, 8, 10), result);
    }

    @Test
    public void testRemoteComputation() {
        CloudSession cloud = CloudSession.builder()
                .master("192.168.1.100", 9090)
                .build();

        // 1. Создаём контекст для работы с облаком
        CloudContext cloudContext = cloud.cloudContext();

        // 📌 Вариант 1: Передача лямбды
        CloudDataset<Integer> dataset = cloudContext.parallelize(List.of(1, 2, 3, 4, 5));
        CloudDataset<Integer> squared = dataset.map(x -> x * x);
        List<Integer> result = squared.collect();
        System.out.println(result);  // [1, 4, 9, 16, 25]

        // 📌 Вариант 2: Передача JAR-файла
        cloudContext.submitJar("my-computation.jar", "com.example.Main", "run");
    }
}
