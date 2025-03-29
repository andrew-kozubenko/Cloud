package ru.nsu.cloud.api;

import ru.nsu.cloud.utils.JarUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class JarExecutionTask implements RemoteTask {
    private byte[] jarBytes;      // Массив байт, содержащий JAR файл
    private String className;      // имя класса для загрузки
    private String methodName;     // имя метода для вызова

    // Конструктор, который принимает байтовый массив (данные JAR) и информацию о классе и методе
    public JarExecutionTask(String jarPath, String className, String methodName) {
        this.className = className;
        this.methodName = methodName;
        this.jarBytes = jarFileToBytes(jarPath);
    }

    private byte[] jarFileToBytes(String jarPath) {
        try {
            return JarUtils.jarFileToBytes(jarPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute() {
        try {
            // Сохраняем JAR файл в локальное место на удаленном компьютере
            File tempJarFile = new File("temp.jar");
            try (FileOutputStream fos = new FileOutputStream(tempJarFile)) {
                fos.write(jarBytes);  // Записываем данные JAR в файл
            }

            // Загружаем JAR в ClassLoader
            URL jarURL = tempJarFile.toURI().toURL();
            try (URLClassLoader classLoader = new URLClassLoader(new URL[]{jarURL}, null)) {

                // Загружаем указанный класс
                Class<?> loadedClass = classLoader.loadClass(className);
                Method method = loadedClass.getMethod(methodName);

                // Вызываем метод без создания объекта (предполагается static метод)
                method.invoke(null);
                System.out.println("Метод выполнен успешно!");

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Удаляем временный файл после выполнения
                tempJarFile.delete();
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Ошибка при выполнении JAR файла: " + e.getMessage(), e);
        }
    }
}
