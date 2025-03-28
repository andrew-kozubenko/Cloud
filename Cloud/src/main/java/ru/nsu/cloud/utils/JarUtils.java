package ru.nsu.cloud.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

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
}
