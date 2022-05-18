package com.largescalesystem.minisql.master;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Objects;

public class Server {
    public static void main(String[] args) throws IOException {
        ServerSocket server = new ServerSocket(2000);
        System.out.println("Master is waiting for connection");
        for(;;){
            Socket client = server.accept();
            ClientHandler clientHandler = new ClientHandler(client);
            clientHandler.start();
        }
    }

    private static class ClientHandler extends Thread{
        private final Socket socket;

        ClientHandler(Socket socket){
            this.socket = socket;
        }

        @Override
        public void run(){
            super.run();
            System.out.println("New client connected, address: " + socket.getInetAddress() + ", port: " + socket.getPort());
            try {
                String sql = String.valueOf(socket.getOutputStream());
                List<String> tableInfo = RegionManager.getNodeList();
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                String tableName = null;
                if (Objects.equals(sql.split(" ")[0], "insert") || Objects.equals(sql.split(" ")[0], "delete")) {
                    tableName = sql.split(" ")[2];
                }
                else if(Objects.equals(sql.split(" ")[0], "select")){
                    tableName = sql.split(" ")[3];

                }
                for(String nodeInfo : tableInfo){
                    String [] list = nodeInfo.split(",");
                    boolean flag = false;
                    for(String s: list){
                        if(Objects.equals(s, tableName) || Objects.equals(s.split("_")[0], tableName)){
                            flag = true;
                            break;
                        }
                    }
                    if(flag){
                        printWriter.println(nodeInfo);
                    }
                }
                printWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
