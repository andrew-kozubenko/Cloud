//package ru.nsu.cloud.master;
//
//import org.junit.jupiter.api.*;
//import ru.nsu.cloud.api.LambdaTask;
//import ru.nsu.cloud.api.RemoteTask;
//import ru.nsu.cloud.api.SerializableFunction;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.*;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class MasterTest {
//    private Master master;
//    private ExecutorService executor;
//
//    @BeforeEach
//    void setUp() {
//        master = new Master(5000); // Порт для тестов (можно заменить)
//        executor = Executors.newSingleThreadExecutor();
//        executor.submit(master::start); // Запускаем Master в отдельном потоке
//    }
//
//    @AfterEach
//    void tearDown() throws Exception {
//        master.stop();
//        executor.shutdown();
//    }
//
//    @Test
//    void testSimpleAdditionTask() throws Exception {
//        // Создаём LambdaTask: прибавляем 5 ко всем элементам списка
//        LambdaTask<Integer, List<Integer>> task = new LambdaTask<>(
//                list -> list.stream().map(x -> x + 5).toList(),
//                List.of(1, 2, 3)
//        );
//
//        // Отправляем задачу в Master
//        Future<Object> future = master.submitTask(task);
//
//        // Проверяем результат
//        assertNotNull(future);
//        List<Integer> result = (List<Integer>) future.get(3, TimeUnit.SECONDS);
//        assertEquals(List.of(6, 7, 8), result);
//    }
//
//    @Test
//    void testStringToUpperCaseTask() throws Exception {
//        // LambdaTask: перевод строки в верхний регистр
//        LambdaTask<String, List<String>> task = new LambdaTask<>(
//                list -> list.stream().map(String::toUpperCase).toList(),
//                List.of("hello", "world")
//        );
//
//        Future<Object> future = master.submitTask(task);
//
//        assertNotNull(future);
//        List<String> result = (List<String>) future.get(3, TimeUnit.SECONDS);
//        assertEquals(List.of("HELLO", "WORLD"), result);
//    }
//
//    @Test
//    void testMultiplicationTask() throws Exception {
////        LambdaTask<Integer, List<Integer>> task = new LambdaTask<>(
////                (List<Integer> input) -> input.stream().map(x -> x * 2).toList(),
////                Arrays.asList(1, 2, 3, 4, 5)  // Входные данные - список чисел
////        );
//        // LambdaTask: умножение всех чисел в списке на 2
//        LambdaTask<Integer, List<Integer>> task = new LambdaTask<>(
//                (List<Integer> input) -> input.stream().map(x -> x * 2).toList(),
//                Arrays.asList(2, 4, 6)
//        );
//
//        Future<Object> future = master.submitTask(task);
//
//        assertNotNull(future);
//        List<Integer> result = (List<Integer>) future.get(3, TimeUnit.SECONDS);
//        assertEquals(List.of(4, 8, 12), result);
//    }
//
//    @Test
//    void testEmptyInputTask() throws Exception {
//        // LambdaTask: суммируем числа, но передаем пустой список
//        LambdaTask<Integer, Integer> task = new LambdaTask<>(
//                list -> list.stream().reduce(0, Integer::sum),
//                List.of()
//        );
//
//        Future<Object> future = master.submitTask(task);
//
//        assertNotNull(future);
//        Integer result = (Integer) future.get(3, TimeUnit.SECONDS);
//        assertEquals(0, result);
//    }
//
//    @Test
//    void testNullInputTask() throws Exception {
//        // LambdaTask: если входные данные null, возвращаем -1
//        LambdaTask<Integer, Integer> task = new LambdaTask<>(
//                list -> (list == null) ? -1 : list.stream().reduce(0, Integer::sum)
//        );
//
//        Future<Object> future = master.submitTask(task);
//
//        assertNotNull(future);
//        Integer result = (Integer) future.get(3, TimeUnit.SECONDS);
//        assertEquals(-1, result);
//    }
//}
