package ru.nsu.cloud.client;

import org.junit.jupiter.api.*;
import ru.nsu.cloud.master.Master;
import ru.nsu.cloud.worker.WorkerNode;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JarExecutionTest {
    private WorkerNode worker;
    private ExecutorService workerExecutor;

    @BeforeEach
    void setUp() throws Exception {
        worker = new WorkerNode("localhost", 9090);
        workerExecutor = Executors.newSingleThreadExecutor();
        workerExecutor.submit(worker::start);
    }

    @AfterEach
    void tearDown() throws IOException {
        worker.stopWorker();
        workerExecutor.shutdown();
    }

    @Test
    public void testJarExecution() throws Exception {
        CloudSession cloud = CloudSession.builder()
                .master("localhost", 9090)
                .build();

        Thread.sleep(1000);

        CloudContext cloudContext = cloud.cloudContext();

        // Получаем путь к JAR-файлу в ресурсах
        URL jarUrl = getClass().getClassLoader().getResource("test.jar");
        if (jarUrl == null) {
            throw new IllegalStateException("JAR file not found in resources");
        }
        Path jarPath = Paths.get(jarUrl.toURI());
        System.out.println(jarPath);

        // Отправляем задачу на удаленное выполнение JAR-а
        String result = (String)cloudContext.submitJar(jarPath.toString(), "org.example.Main", "mainResult");

        cloud.stop();

        assertEquals("7x^3 + 6x^2 + 19x + 6", result);
    }
}
