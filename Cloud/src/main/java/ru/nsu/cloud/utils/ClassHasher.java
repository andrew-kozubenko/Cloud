package ru.nsu.cloud.utils;

import java.security.MessageDigest;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ClassHasher {
    public static void main(String[] args) throws Exception {
        System.out.println("Current dir: " + Paths.get("").toAbsolutePath());

        byte[] classBytes = Files.readAllBytes(
                Paths.get("build/classes/java/main/ru/nsu/cloud/api/LambdaTask.class")
        );

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(classBytes);

        System.out.println("LambdaTask MD5: " + bytesToHex(digest));
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
