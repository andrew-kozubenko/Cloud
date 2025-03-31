package ru.nsu.cloud.client;

import ru.nsu.cloud.api.SerializableFunction;
import ru.nsu.cloud.master.Master;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;

public class CloudDataset<T> implements Serializable {
    private final Master master;
    private final List<T> data;

    public CloudDataset(Master master, List<T> data) {
        this.master = master;
        this.data = data;
    }

    /**
     * Применяет функцию преобразования к каждому элементу
     */
    public <R> CloudDataset<R> map(SerializableFunction<T, R> func) {
        try {
            // Дожидаемся результата
            List<R> result = master.remoteMap(func, data).get();
            return new CloudDataset<>(master, result);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Ошибка при выполнении remoteMap", e);
        }
    }


    /**
     * Фильтрует элементы по предикату
     */
    public CloudDataset<T> filter(Predicate<T> predicate) {
        return null;
        //return new CloudDataset<>(master, master.distributedFilter(data, new RemoteTask<>(predicate)));
    }

    /**
     * Собирает результат выполнения обратно в список
     */
    public List<T> collect() {
        return data;
    }
}
