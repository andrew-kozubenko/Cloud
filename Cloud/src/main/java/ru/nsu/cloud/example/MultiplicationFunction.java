package ru.nsu.cloud.example;

import ru.nsu.cloud.api.SerializableFunction;

import java.util.List;

public class MultiplicationFunction implements SerializableFunction<Object, Integer> {
    @Override
    public Integer apply(Object input) {
        List<Integer> inputList = (List<Integer>) input;
        return inputList.stream().mapToInt(i -> i * 2).sum();
    }
}
