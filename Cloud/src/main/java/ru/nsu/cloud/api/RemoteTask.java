package ru.nsu.cloud.api;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

public interface RemoteTask extends Serializable {
    void execute();
}


