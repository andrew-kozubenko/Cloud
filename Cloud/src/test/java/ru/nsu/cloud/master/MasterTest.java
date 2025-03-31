package ru.nsu.cloud.master;

import ru.nsu.cloud.api.LambdaTask;
import ru.nsu.cloud.api.SerializableFunction;
import ru.nsu.cloud.worker.WorkerNode;
import ru.nsu.cloud.api.RemoteTask;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MasterTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        // Создаем мастер-сервер
        Master master = new Master(9090);
        // Запускаем мастер-сервер в отдельном потоке
        new Thread(() -> {
            master.start();
        }).start();

        // Даем время на запуск мастера
        Thread.sleep(2000);  // Подождем 2 секунды, чтобы убедиться, что мастер начал слушать порт

        // Запускаем воркера
        WorkerNode worker = new WorkerNode("localhost", 9090);
        new Thread(() -> worker.start()).start();

        // Даем немного времени на подключение воркера
        Thread.sleep(2000);  // Подождем еще немного, чтобы воркер подключился к мастеру

        // Запускаем тест с умножением
        testLambdaTaskWithMultiplication(master);

        // Даем время для выполнения задач
        Thread.sleep(5000);

        // Завершаем работу
        master.stop();
        worker.stopWorker();
    }

    public static void testLambdaTaskWithMultiplication(Master master) throws ExecutionException, InterruptedException {
        // Лямбда для умножения всех чисел на 2
        SerializableFunction<Object, Integer> multiplicationFunction = (input) -> {
            List<Integer> inputList = (List<Integer>) input;
            return inputList.stream().mapToInt(i -> i * 2).sum();  // Умножаем все числа на 2 и суммируем
        };

        // Входные данные
        List<Integer> inputData = IntStream.range(1, 6).boxed().collect(Collectors.toList());  // [1, 2, 3, 4, 5]

        // Создаем задачу
        LambdaTask<Integer> task = new LambdaTask<>(multiplicationFunction, inputData);

        // Отправляем задачу мастеру
        Future<Object> future = master.submitTask(task);

        // Получаем результат
        Integer result = (Integer) future.get();  // Ожидаем результата умножения

        // Выводим результат
        System.out.println("Result of multiplication task: " + result);  // Ожидаем: 2 + 4 + 6 + 8 + 10 = 30
    }
}
