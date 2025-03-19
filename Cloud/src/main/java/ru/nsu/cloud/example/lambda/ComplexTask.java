package ru.nsu.cloud.example.lambda;

import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.api.SerializableFunction;

import java.util.List;

public class ComplexTask implements RemoteTask<Integer, Integer> {
    private transient SerializableFunction<Integer, Integer> square;
    private transient SerializableFunction<Integer, Integer> addFive;

    @Override
    public Integer apply(Integer input) { // Теперь используем apply вместо execute
        return square.apply(input) + addFive.apply(input);
    }

    @Override
    public void setDependencies(List<SerializableFunction<?, ?>> dependencies) {
        square = (SerializableFunction<Integer, Integer>) dependencies.get(0);
        addFive = (SerializableFunction<Integer, Integer>) dependencies.get(1);
    }
}
