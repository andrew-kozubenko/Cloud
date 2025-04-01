package ru.nsu.cloud.client;

import org.junit.jupiter.api.*;
import ru.nsu.cloud.api.LambdaTask;
import ru.nsu.cloud.api.SerializableFunction;
import ru.nsu.cloud.master.Master;
import ru.nsu.cloud.worker.WorkerNode;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ClientTest {
    private WorkerNode worker;
    private ExecutorService workerExecutor;

    @BeforeEach
    void setUp() throws Exception {
        worker = new WorkerNode("localhost", 9090);
        workerExecutor = Executors.newSingleThreadExecutor();
        workerExecutor.submit(worker::start);
    }

    @AfterEach
    void tearDown() throws IOException {
        worker.stopWorker();
        workerExecutor.shutdown();
    }

    @Test
    public void testRemoteComputation() throws IOException, InterruptedException {
        CloudSession cloud = CloudSession.builder()
                .master("192.168.1.100", 9090)
                .build();

        Thread.sleep(1000);

        // 1. Создаём контекст для работы с облаком
        CloudContext cloudContext = cloud.cloudContext();

        // 2. Загружаем список данных в CloudDataset
        List<Integer> data = List.of(1, 2, 3, 4, 5);
        var dataset = cloudContext.parallelize(data);

        // 3. Применяем удалённое вычисление (умножаем на 2)
        var transformedDataset = dataset.map(x -> x * 2);

        // 4. Собираем результат
        List<Integer> result = transformedDataset.collect();

        cloud.stop();

        // 5. Проверяем, что все элементы умножились на 2
        assertEquals(List.of(2, 4, 6, 8, 10), result);
    }

    @Test
    public void testRemoteComputationLargeDataset() throws InterruptedException, IOException {
        CloudSession cloud = CloudSession.builder()
                .master("192.168.1.100", 9090)
                .build();

        Thread.sleep(1000);

        // 1. Создаём контекст для работы с облаком
        CloudContext cloudContext = cloud.cloudContext();

        // 2. Генерируем большой массив данных (10_000 элементов)
        List<Integer> data = IntStream.range(1, 10_001).boxed().toList();
        var dataset = cloudContext.parallelize(data);

        // 3. Применяем удалённое вычисление (каждое число умножается на 2)
        var transformedDataset = dataset.map(x -> x * 2);

        // 4. Собираем результат
        List<Integer> result = transformedDataset.collect();

        // 5. Проверяем, что все элементы умножились на 2
        List<Integer> expected = IntStream.range(1, 10_001).map(x -> x * 2).boxed().toList();

        cloud.stop();

        assertEquals(expected, result);
    }

    @Test
    public void testLambdaTask() throws InterruptedException, ExecutionException, IOException {
        CloudSession cloud = CloudSession.builder()
                .master("192.168.1.100", 9090)
                .build();

        Thread.sleep(1000);

        // 1. Создаём контекст для работы с облаком
        CloudContext cloudContext = cloud.cloudContext();

        SerializableFunction<Object, Integer> multiplicationFunction = (input) -> {
            List<Integer> inputList = (List<Integer>) input;
            return inputList.stream().mapToInt(i -> i * 2).sum();
        };

        List<Integer> inputData = List.of(1, 2, 3, 4, 5);
        LambdaTask<Integer> task = new LambdaTask<>(multiplicationFunction, inputData);

        Integer result = (Integer) cloudContext.submitTask(task);

        cloud.stop();

        assertEquals(30, result, "Multiplication task should return 30");
    }
}
