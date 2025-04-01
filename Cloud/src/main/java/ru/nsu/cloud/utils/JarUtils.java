package ru.nsu.cloud.utils;

import java.io.*;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class JarUtils {

    /**
     * Преобразует JAR-файл в массив байт.
     *
     * @param jarFilePath Путь до JAR-файла.
     * @return Массив байт, представляющий содержимое JAR-файла.
     * @throws IOException Если произошла ошибка при чтении файла.
     */
    public static byte[] jarFileToBytes(String jarFilePath) throws IOException {
        File jarFile = new File(jarFilePath);
        try (FileInputStream fis = new FileInputStream(jarFile);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            return baos.toByteArray();
        }
    }

    public static byte[] createJarAsBytes(Set<Class<?>> dependencies) {
        try {
            ByteArrayOutputStream jarOutputStream = new ByteArrayOutputStream();
            try (JarOutputStream jarOut = new JarOutputStream(jarOutputStream)) {
                for (Class<?> clazz : dependencies) {
                    String path = clazz.getName().replace('.', '/') + ".class";
                    InputStream classStream = clazz.getClassLoader().getResourceAsStream(path);
                    if (classStream != null) {
                        jarOut.putNextEntry(new JarEntry(path));
                        jarOut.write(classStream.readAllBytes());
                        jarOut.closeEntry();
                    }
                }
            }
            return jarOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error creating JAR", e);
        }
    }
}
