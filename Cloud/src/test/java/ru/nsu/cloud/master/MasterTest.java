package ru.nsu.cloud.master;

import org.junit.jupiter.api.*;
import ru.nsu.cloud.api.LambdaTask;
import ru.nsu.cloud.api.SerializableFunction;
import ru.nsu.cloud.worker.WorkerNode;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MasterTest {
    private Master master;
    private WorkerNode worker;
    private ExecutorService workerExecutor;

    @BeforeEach
    void setUp() throws Exception {
        master = new Master(9090);
        new Thread(master::start).start();

        // Ожидаем, пока мастер начнет слушать порт
        Thread.sleep(1000);

        worker = new WorkerNode("localhost", 9090);
        workerExecutor = Executors.newSingleThreadExecutor();
        workerExecutor.submit(worker::start);

        // Ожидаем подключения воркера
        Thread.sleep(1000);
    }

    @AfterEach
    void tearDown() throws IOException {
        worker.stopWorker();
        master.stop();
        workerExecutor.shutdown();
    }

    @Test
    void testLambdaTaskWithMultiplication() throws Exception {
        // Лямбда-функция: умножаем все числа на 2 и суммируем
        SerializableFunction<Object, Integer> multiplicationFunction = (input) -> {
            List<Integer> inputList = (List<Integer>) input;
            return inputList.stream().mapToInt(i -> i * 2).sum();
        };

        List<Integer> inputData = List.of(1, 2, 3, 4, 5);
        LambdaTask<Integer> task = new LambdaTask<>(multiplicationFunction, inputData);

        Future<Object> future = master.submitTask(task);
        Integer result = (Integer) future.get();

        assertEquals(30, result, "Multiplication task should return 30");
    }
}
