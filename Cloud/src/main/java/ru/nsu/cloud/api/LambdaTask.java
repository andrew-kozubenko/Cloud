package ru.nsu.cloud.api;

import java.util.List;

public class LambdaTask<T, R> extends RemoteTask<R> {
    private final SerializableFunction<List<T>, R> function; // Лямбда-функция принимает список T и возвращает R
    private final List<T> input; // Входные данные (список элементов типа T)

    // Конструктор с входными данными
    public LambdaTask(SerializableFunction<List<T>, R> function, List<T> input) {
        this.function = function;
        this.input = input;
    }

    // Конструктор без входных данных (если лямбда не требует их)
    public LambdaTask(SerializableFunction<List<T>, R> function) {
        this(function, null);  // В таком случае передаем null
    }

    @Override
    public R execute() {
        try {
            // Если входные данные есть, передаем их в лямбду, если нет, передаем null.
            R result = (input != null) ? function.apply(input) : function.apply(null);

            // Выводим результат выполнения лямбды
            System.out.println("Lambda executed, result: " + result);

            return result;
        } catch (Exception e) {
            // Логируем ошибку, если выполнение лямбды вызвало исключение
            System.err.println("Error during Lambda execution: " + e.getMessage());
            e.printStackTrace(); // Выводим stack trace ошибки
            return null; // Можно вернуть null, чтобы показать, что выполнение не получилось
        }
    }
}
