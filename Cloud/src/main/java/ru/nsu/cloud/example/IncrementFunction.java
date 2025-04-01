package ru.nsu.cloud.example;

import ru.nsu.cloud.api.SerializableFunction;

public class IncrementFunction implements SerializableFunction<Object, Integer> {
    @Override
    public Integer apply(Object input) {
        return ((Integer) input) + 1;
    }
}
