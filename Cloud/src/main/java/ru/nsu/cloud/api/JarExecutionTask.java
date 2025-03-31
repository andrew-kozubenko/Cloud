package ru.nsu.cloud.api;

import ru.nsu.cloud.utils.JarUtils;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class JarExecutionTask extends RemoteTask<Object> {  // Тип возвращаемого значения Object
    private byte[] jarBytes;      // Массив байт, содержащий JAR файл
    private String className;      // Имя класса для загрузки
    private String methodName;     // Имя метода для вызова

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
            throw new RuntimeException("Error when converting JAR to bytes: " + e.getMessage(), e);
        }
    }

    @Override
    public Object execute() {
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

                // Вызываем метод и получаем результат
                Object result = method.invoke(null);  // Метод должен быть static
                System.out.println("The method was completed successfully, the result is: " + result);

                // Возвращаем результат выполнения метода
                return result;

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Error when calling a method from a JAR file: " + e.getMessage(), e);
            } finally {
                // Удаляем временный файл после выполнения
                tempJarFile.delete();
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error saving JAR file: " + e.getMessage(), e);
        }
    }
}
