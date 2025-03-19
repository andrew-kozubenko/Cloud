package ru.nsu.cloud.master;

import org.junit.jupiter.api.*;
import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MasterNodeTest {
    private static int TEST_PORT;
    private static ExecutorService workerExecutor;

    @BeforeEach
    void setUp() throws IOException {
        TEST_PORT = getFreePort();
        workerExecutor = Executors.newSingleThreadExecutor();
        startMockWorker(TEST_PORT);
    }

    @AfterEach
    void tearDown() {
        workerExecutor.shutdownNow();
    }

    private int getFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void startMockWorker(int port) {
        workerExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("✅ Mock Worker is running on the port: " + port);
                boolean running = true;

                while (running) {
                    try (Socket socket = serverSocket.accept();
                         ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                         ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

                        Object received;
                        while ((received = ois.readObject()) != null) {
                            if ("SHUTDOWN".equals(received)) {
                                System.out.println("Mock Worker got SHUTDOWN");
                                running = false;
                                break;
                            }

                            if ("HEARTBEAT".equals(received)) {
                                oos.writeObject("ALIVE");
                                oos.flush();
                                continue;
                            }

                            @SuppressWarnings("unchecked")
                            RemoteTask<Integer, Integer> task = (RemoteTask<Integer, Integer>) received;
                            Integer input = (Integer) ois.readObject();
                            Integer result = task.apply(input);
                            oos.writeObject(result);
                            oos.flush();
                        }
                    } catch (EOFException ignored) {
                    } catch (Exception e) {
                        if (!running) {
                            System.out.println("Mock Worker is coming to an end...");
                            break;
                        }
                        e.printStackTrace();
                    }
                }

                System.out.println("Mock Worker shuts down...");
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
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
