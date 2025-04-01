package ru.nsu.cloud.api;

import ru.nsu.cloud.utils.JarUtils;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class JarExecutionTask extends RemoteTask<Object> {  // Тип возвращаемого значения Object
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
            throw new RuntimeException("Error when converting JAR to bytes: " + e.getMessage(), e);
        }
    }

    @Override
    public Object execute() {
        try {
            // Получаем путь к рабочему столу текущего пользователя
            String desktopPath = System.getProperty("user.home") + File.separator + "Desktop";
            File tempJarFile = new File(desktopPath, "temp.jar");

            // Сохраняем JAR файл в локальное место на удаленном компьютере
            try (FileOutputStream fos = new FileOutputStream(tempJarFile)) {
                fos.write(jarBytes);  // Записываем данные JAR в файл
            }

            System.out.println("JAR saved: " + tempJarFile.getAbsolutePath());

            // Загружаем JAR в ClassLoader
            URL jarURL = tempJarFile.toURI().toURL();
            try (URLClassLoader classLoader = new URLClassLoader(new URL[]{jarURL}, this.getClass().getClassLoader())) {

                System.out.println("Getting class...");
                // Загружаем указанный класс
                Class<?> loadedClass = Class.forName(className, true, classLoader);
                System.out.println("CLASS");
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
