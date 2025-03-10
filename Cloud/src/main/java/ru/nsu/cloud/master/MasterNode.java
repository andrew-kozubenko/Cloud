package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.Socket;

public class MasterNode {
    public static void main(String[] args) {
        try (Socket socket = new Socket("localhost", 5000);
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

            RemoteTask<Integer, Integer> task = (x) -> x * 2; // удаленная функция
            oos.writeObject(task);
            oos.writeInt(10); // отправляем данные
            oos.flush();

            int result = ois.readInt();
            System.out.println("Result from worker: " + result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

