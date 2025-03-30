package ru.nsu.cloud.api;

import java.io.Serializable;
import java.util.UUID;

public abstract class RemoteTask<T> implements Serializable {
    private final String id;  // Уникальный идентификатор задачи

    public RemoteTask() {
        this.id = UUID.randomUUID().toString(); // Генерируем уникальный ID
    }

    public String getId() {
        return id;
    }

    public abstract T execute();
}


