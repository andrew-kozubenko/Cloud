package ru.nsu.cloud.client;

import ru.nsu.cloud.master.Master;

import java.io.Serializable;
import java.util.List;
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
    public <R> CloudDataset<R> map(Function<T, R> func) {
        return new CloudDataset<>(master, master.distributedMap(data, new RemoteTask<>(func)));
    }

    /**
     * Фильтрует элементы по предикату
     */
    public CloudDataset<T> filter(Predicate<T> predicate) {
        return new CloudDataset<>(master, master.distributedFilter(data, new RemoteTask<>(predicate)));
    }

    /**
     * Собирает результат выполнения обратно в список
     */
    public List<T> collect() {
        return master.distributedMap(data, new RemoteTask<>(x -> x));
    }
}
