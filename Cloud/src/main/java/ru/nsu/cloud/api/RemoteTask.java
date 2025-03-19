package ru.nsu.cloud.api;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

public interface RemoteTask<T, R> extends Function<T, R>, Serializable {
    default void setDependencies(List<SerializableFunction<?, ?>> dependencies) {
        // По умолчанию ничего не делает, но конкретные задачи могут переопределять этот метод
    }
}


