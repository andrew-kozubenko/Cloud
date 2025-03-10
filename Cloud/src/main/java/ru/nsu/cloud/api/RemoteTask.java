package ru.nsu.cloud.api;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface RemoteTask<T, R> extends Function<T, R>, Serializable {
    // Это позволяет передавать лямбда-выражения и объекты между узлами
}

