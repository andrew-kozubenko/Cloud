package ru.nsu.cloud.api;

public class LambdaTask implements RemoteTask {
    private final SerializableFunction<?, ?> function;

    public LambdaTask(SerializableFunction<?, ?> function) {
        this.function = function;
    }

    @Override
    public void execute() {
        // Выполнить лямбду
        System.out.println(function.apply(null));  // Здесь можно вызвать на каких-либо данных
    }
}
