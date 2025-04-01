package ru.nsu.cloud.api;

import ru.nsu.cloud.worker.WorkerNode;

import java.util.List;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.util.logging.Level;
import java.util.logging.Logger;


public class LambdaTask<R> extends RemoteTask<R> implements Serializable {
    private static final Logger logger = Logger.getLogger(LambdaTask.class.getName());
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
            logger.info("Start execute");
            SerializableFunction<Object, R> realFunction;

            if (function instanceof SerializableFunction) {
                realFunction = (SerializableFunction<Object, R>) function;
            } else if (function instanceof SerializedLambda) {
                logger.info("Received SerializedLambda instead of SerializableFunction!");
                throw new IllegalStateException("Cannot cast SerializedLambda to SerializableFunction");
            } else {
                throw new IllegalStateException("Unknown function type: " + function.getClass().getName());
            }

            // Выполняем лямбду с входными данными (если они есть)
            R result = (input != null) ? realFunction.apply(input) : realFunction.apply(null);
            logger.info("Lambda executed, result: " + result);
            return result;

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error during Lambda execution: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
