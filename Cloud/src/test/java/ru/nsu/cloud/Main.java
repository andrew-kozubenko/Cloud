package ru.nsu.cloud;

import ru.nsu.cloud.api.LambdaTask;
import ru.nsu.cloud.client.CloudContext;
import ru.nsu.cloud.client.CloudSession;
import ru.nsu.cloud.example.MultiplicationFunction;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        CloudSession cloud = CloudSession.builder()
                .master("192.168.1.100", 9090)
                .build();

        Thread.sleep(1000);

        // 1. Создаём контекст для работы с облаком
        CloudContext cloudContext = cloud.cloudContext();

        MultiplicationFunction multiplicationFunction = new MultiplicationFunction();

        List<Integer> inputData = List.of(1, 2, 3, 4, 5);
        LambdaTask<Integer> task = new LambdaTask<>(multiplicationFunction, inputData);

        Integer result = (Integer) cloudContext.submitTask(task);

        cloud.stop();
    }
}
