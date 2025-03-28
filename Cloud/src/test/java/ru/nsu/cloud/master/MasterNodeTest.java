package ru.nsu.cloud.master;

import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import ru.nsu.cloud.api.RemoteTask;
import ru.nsu.cloud.api.SerializableFunction;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MasterNodeTest {
    private MasterNode masterNode;
    private WorkerNodeInfo mockWorker;
    private Socket mockSocket;
    private ObjectOutputStream mockOos;
    private ExecutorService mockExecutor;

    @BeforeEach
    void setUp() throws Exception {
        masterNode = new MasterNode();

        // Мокируем воркера
        mockWorker = mock(WorkerNodeInfo.class);
        when(mockWorker.isAvailable()).thenReturn(true);
        when(mockWorker.getHost()).thenReturn("localhost");
        when(mockWorker.getPort()).thenReturn(1234);

        // Мокируем сокет и потоки
        mockSocket = mock(Socket.class);
        mockOos = mock(ObjectOutputStream.class);
        when(mockSocket.getOutputStream()).thenReturn(mockOos);

        // Подменяем карту сокетов
        masterNode.getWorkerSockets().put(mockWorker, mockSocket);

        // Мокируем выбор воркера
        MasterNode spyMaster = spy(masterNode);
        doReturn(mockWorker).when(spyMaster).selectWorker();
        masterNode = spyMaster;

        // Подменяем ExecutorService, чтобы избежать реального выполнения
        mockExecutor = mock(ExecutorService.class);
        masterNode.setTaskExecutor(mockExecutor);
    }


    void testSendTaskToWorker() throws Exception {
        // Мокируем задачу и зависимости
        RemoteTask<String, String> mockTask = mock(RemoteTask.class);
        List<SerializableFunction<?, ?>> dependencies = Collections.emptyList();
        List<String> inputBatch = List.of("Task1", "Task2");
        String taskId = "test-task-id";

        // Вызываем sendTaskToWorker
        masterNode.sendTaskToWorker(mockTask, dependencies, inputBatch, taskId);

        // Захватываем переданный Runnable в ExecutorService
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutor).submit(captor.capture());

        // Запускаем захваченный Runnable (имитируем выполнение потока)
        captor.getValue().run();

        // Проверяем, что сокет использовался и данные отправлены
        verify(mockOos, times(1)).writeObject(mockTask);
        verify(mockOos, times(1)).writeObject(dependencies);
        verify(mockOos, times(1)).writeObject(inputBatch);
        verify(mockOos, times(1)).flush();

        // Проверяем запуск потока для чтения ответа
        verify(mockExecutor, times(2)).submit(any(Runnable.class));
    }

    @Test
    void testShutdownWorkers() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        master.shutdownWorkers(); // Отправляем команду SHUTDOWN
        Thread.sleep(100); // Ждем завершения

        // Проверяем, что воркер больше не отвечает
        try (Socket socket = new Socket("localhost", TEST_PORT);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
            oos.writeObject("HEARTBEAT");
            oos.flush();
            String response = (String) ois.readObject();
            fail("The worker was expected to be disabled, but it is still responding: " + response);
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("✅ The Worker has really disconnected!");
        }
    }

    @Test
    void testWorkerTimeoutHandling() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> slowTask = (input) -> {
            try {
                Thread.sleep(5000); // Долгая задача (5 секунд)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return input * 2;
        };

        String taskId = master.submitTask(slowTask);
        master.sendTaskToWorker(slowTask, 5, taskId);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Object> future = executor.submit(master::getNextResult);

        try {
            Object result = future.get(2, TimeUnit.SECONDS);
            fail("Expected timeout, but got: " + result);
        } catch (TimeoutException e) {
            System.out.println("✅ TimeoutException caught as expected!");
        } catch (ExecutionException e) {
            System.err.println("❌ ExecutionException: " + e.getCause().getMessage());
            fail("Unexpected ExecutionException occurred!");
        } finally {
            executor.shutdown();
        }
    }


    @Test
    void testSendTaskToWorkerWithCallback() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;
        AtomicReference<Object> callbackResult = new AtomicReference<>();

        String taskId = master.submitTask(addTen);
        master.registerCallback(taskId, callbackResult::set);

        master.sendTaskToWorker(addTen, 5, taskId);

        Thread.sleep(100); // Ждем обработку

        assertEquals(15, callbackResult.get());
    }


    @Test
    void testSubmitAndRetrieveTasks() {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;
        RemoteTask<String, String> toUpperCase = String::toUpperCase;

        master.submitTask(addTen);
        master.submitTask(toUpperCase);

        @SuppressWarnings("unchecked")
        RemoteTask<Integer, Integer> task1 = (RemoteTask<Integer, Integer>) master.getNextTask();
        @SuppressWarnings("unchecked")
        RemoteTask<String, String> task2 = (RemoteTask<String, String>) master.getNextTask();

        assertEquals(15, task1.apply(5));
        assertEquals("HELLO", task2.apply("hello"));
    }

    @Test
    void testResultAddedToQueue() {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        String taskId = master.submitTask(addTen);
        master.sendTaskToWorker(addTen, 5, taskId);

        // Ждем и проверяем, что результат попал в очередь
        Object result = master.getNextResult();
        assertEquals(15, result);
    }

    @Test
    void testCallbackExecution() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> addTen = (input) -> input + 10;

        AtomicReference<Object> callbackResult = new AtomicReference<>();
        String taskId = master.submitTask(addTen);
        master.registerCallback(taskId, result -> {
            System.out.println("Callback received result: " + result);
            callbackResult.set(result);
        });

        master.sendTaskToWorker(addTen, 5, taskId);

        // Ожидание для обработки колбэка
        Thread.sleep(100);

        assertEquals(15, callbackResult.get());
    }

    @Test
    void testDistributedMap() throws InterruptedException {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> multiplyByTwo = (input) -> input * 2;

        List<Integer> inputs = List.of(1, 2, 3, 4, 5);
        List<Integer> results = master.distributedMap(inputs, multiplyByTwo);

        assertEquals(List.of(2, 4, 6, 8, 10), results);
    }

    @Test
    void testDistributedStreamMap() {
        MasterNode master = new MasterNode();
        master.registerWorker("localhost", TEST_PORT); // Регистрация воркера ✅

        RemoteTask<Integer, Integer> square = (input) -> input * input;

        Stream<Integer> inputStream = Stream.of(2, 3, 4);
        Stream<Integer> resultStream = master.distributedStreamMap(inputStream, square);

        assertEquals(List.of(4, 9, 16), resultStream.toList());
    }
}
