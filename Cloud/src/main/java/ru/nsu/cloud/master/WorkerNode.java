package ru.nsu.cloud.master;

import ru.nsu.cloud.api.RemoteTask;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class WorkerNode {
    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(5000)) {
            System.out.println("Worker node is running...");

            while (true) {
                try (Socket socket = serverSocket.accept();
                     ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                     ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

                    RemoteTask<Integer, Integer> task = (RemoteTask<Integer, Integer>) ois.readObject();
                    int input = ois.readInt();

                    int result = task.apply(input);
                    oos.writeInt(result);
                    oos.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
