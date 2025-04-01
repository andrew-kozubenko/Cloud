package ru.nsu.cloud.example;

import ru.nsu.cloud.api.SerializableFunction;

public class MultByTwo implements SerializableFunction<Integer, Integer> {
    @Override
    public Integer apply(Integer x) {
        return x * 2;
    }
}
