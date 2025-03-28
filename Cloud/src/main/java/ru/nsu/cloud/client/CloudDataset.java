package ru.nsu.cloud.client;

import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.api.SerializableFunction;
import ru.nsu.cloud.master.MasterNode;
import ru.nsu.cloud.utils.DependencyCollector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class CloudDataset<T> {
    private final MasterNode masterNode;
    private final List<T> data;
    private final List<SerializableFunction<?, ?>> transformations = new ArrayList<>();

    public CloudDataset(MasterNode masterNode, List<T> data) {
        this.masterNode = masterNode;
        this.data = data;
    }

    public <R> CloudDataset<R> map(SerializableFunction<T, R> function) {
        CloudDataset<R> newDataset = new CloudDataset<>(masterNode, null);
        newDataset.transformations.addAll(this.transformations);
        newDataset.transformations.add(function);

        // Собираем зависимости и передаём их на мастер
        Set<Class<?>> dependencies = new HashSet<>();
        DependencyCollector.collectDependencies(function, dependencies);
        masterNode.registerDependencies(dependencies);

        return newDataset;
    }


    public List<T> collect() {
        return masterNode.distributedMap(data, new RemoteTask<>(transformations), transformations);
    }
}
