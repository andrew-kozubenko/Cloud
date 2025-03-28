package ru.nsu.cloud.utils;

import java.lang.reflect.Field;
import java.util.Set;

public class DependencyCollector {
    public static void collectDependencies(Object function, Set<Class<?>> dependencies) {
        if (function == null) return;
        Class<?> clazz = function.getClass();

        // Добавляем текущий класс в зависимости
        dependencies.add(clazz);

        // Проверяем все поля (возможные захваченные переменные)
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            try {
                Object fieldValue = field.get(function);
                if (fieldValue != null && !dependencies.contains(fieldValue.getClass())) {
                    collectDependencies(fieldValue, dependencies);
                }
            } catch (IllegalAccessException ignored) {}
        }
    }

}
