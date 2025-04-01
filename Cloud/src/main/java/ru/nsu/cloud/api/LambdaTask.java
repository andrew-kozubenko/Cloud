package ru.nsu.cloud.api;

import java.util.List;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;

public class LambdaTask<R> extends RemoteTask<R> implements Serializable {
    private final Object function; // Может быть либо SerializableFunction, либо SerializedLambda
    private final Object input; // Входные данные для лямбды (может быть null или List)

    // Конструктор с входными данными
    public LambdaTask(SerializableFunction<Object, R> function, Object input) {
        this.function = function;
        this.input = input;
    }

    // Конструктор без входных данных
    public LambdaTask(SerializableFunction<Object, R> function) {
        this(function, null);
    }

    @Override
    public R execute() {
        try {
            SerializableFunction<Object, R> realFunction;

            if (function instanceof SerializableFunction) {
                realFunction = (SerializableFunction<Object, R>) function;
            } else if (function instanceof SerializedLambda) {
                System.out.println("Received SerializedLambda instead of SerializableFunction!");
                throw new IllegalStateException("Cannot cast SerializedLambda to SerializableFunction");
            } else {
                throw new IllegalStateException("Unknown function type: " + function.getClass().getName());
            }

            // Выполняем лямбду с входными данными (если они есть)
            R result = (input != null) ? realFunction.apply(input) : realFunction.apply(null);
            System.out.println("Lambda executed, result: " + result);
            return result;

        } catch (Exception e) {
            System.err.println("Error during Lambda execution: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
