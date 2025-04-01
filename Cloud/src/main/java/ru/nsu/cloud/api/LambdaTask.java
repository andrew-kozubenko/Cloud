package ru.nsu.cloud.api;

import ru.nsu.cloud.utils.DependencyCollector;
import ru.nsu.cloud.utils.JarUtils;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LambdaTask<R> extends RemoteTask<R> implements Serializable {
    private static final Logger logger = Logger.getLogger(LambdaTask.class.getName());

    private final byte[] functionData;
    private final Object input;
    private final byte[] jarData; // JAR с классами

    public LambdaTask(SerializableFunction<Object, R> function, Object input) {
        this.input = input;

        // Собираем зависимости
        Set<Class<?>> dependencies = new HashSet<>();
        DependencyCollector.collectDependencies(function, dependencies);

        // Создаём JAR с зависимостями
        this.jarData = JarUtils.createJarAsBytes(dependencies);

        // Сериализуем функцию
        this.functionData = serializeFunction(function);
    }

    public LambdaTask(SerializableFunction<Object, R> function) {
        this(function, null);
    }

    @Override
    public R execute() {
        try {
            logger.info("Start execute");

            // Разархивируем JAR во временную папку
            File jarFile = extractJarToTemp(jarData);
            addJarToClasspath(jarFile);

            // Десериализуем функцию
            SerializableFunction<Object, R> realFunction = (SerializableFunction<Object, R>) deserializeFunction(functionData);

            // Выполняем лямбду
            R result = (input != null) ? realFunction.apply(input) : realFunction.apply(null);
            logger.info("Lambda executed, result: " + result);
            return result;

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error during Lambda execution: " + e.getMessage(), e);
            return null;
        }
    }

    private File extractJarToTemp(byte[] jarBytes) throws IOException {
        File tempJar = File.createTempFile("dependencies", ".jar");
        try (FileOutputStream fos = new FileOutputStream(tempJar)) {
            fos.write(jarBytes);
        }
        tempJar.deleteOnExit(); // Удаляем файл после завершения программы
        return tempJar;
    }

    private static byte[] serializeFunction(SerializableFunction<?, ?> function) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(function);
            return bos.toByteArray();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error serializing function", e);
            return null;
        }
    }

    private static Object deserializeFunction(byte[] data) {
        if (data == null) {
            logger.severe("Function data is null, cannot deserialize.");
            return null;
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error deserializing function", e);
            return null;
        }
    }

    private void addJarToClasspath(File jarFile) throws Exception {
        // Получаем URL для JAR
        URL jarUrl = jarFile.toURI().toURL();

        // Создаём URLClassLoader с нашим JAR
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{jarUrl}, getClass().getClassLoader());

        // Делаем его доступным для текущего потока (если необходимо)
        Thread.currentThread().setContextClassLoader(urlClassLoader);

        // Загружаем все классы из JAR
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();

            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();

                // ищем только файлы .class
                if (entry.getName().endsWith(".class")) {
                    String className = entry.getName().replace('/', '.').substring(0, entry.getName().length() - 6);
                    try {
                        // Пытаемся загрузить каждый класс
                        Class<?> clazz = urlClassLoader.loadClass(className);
                        logger.info("Class loaded: " + clazz.getName());
                    } catch (ClassNotFoundException e) {
                        logger.log(Level.WARNING, "Class not found in the JAR: " + className, e);
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error reading JAR file", e);
            throw e;
        }
    }
}
