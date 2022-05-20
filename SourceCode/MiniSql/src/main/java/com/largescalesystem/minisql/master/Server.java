package com.largescalesystem.minisql.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Objects;

public class Server {
    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(2000);
        System.out.println("Master is waiting for connection");
        for(;;){
            Socket client = server.accept();
            ClientHandler clientHandler = new ClientHandler(client);
            clientHandler.start();
            RegionManager.listenNode();
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
                // 监听节点
//                RegionManager.listenNode();
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String temp = input.readLine();
                String sql = input.readLine();
                System.out.println("CLIENT REQUEST: " + sql);
                ArrayList<String> tableInfo = RegionManager.getNodeList();
//                ArrayList<String> tableInfo = new ArrayList<>();
//                tableInfo.add("127.0.0.1,1001,3306,123,root,3,school,student,teacher_slave");
//                tableInfo.add("127.0.0.1,1001,3306,123,root,3,school,student,teacher_slave");
//                System.out.println(tableInfo);
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                String tableName = null;
                if (Objects.equals(sql.split(" ")[0], "insert") || Objects.equals(sql.split(" ")[0], "delete")) {
                    tableName = sql.split(" ")[2];
                }
                else if(Objects.equals(sql.split(" ")[0], "select")){
                    tableName = sql.split(" ")[3];
                }
                else if(Objects.equals(sql.split(" ")[0], "create")){
//                    tableName = sql.split(" ")[2];
                    tableName = sql.split("\\(")[0].split(" ")[2];
                }

//                System.out.println(tableName);
                if(Objects.equals(sql.split(" ")[0], "create")){
                    System.out.println("TARGET REGION SERVER: " + RegionManager.loadBalance(tableInfo));
                    printWriter.println(RegionManager.loadBalance(tableInfo));
                }
                else {
                    for (String nodeInfo : tableInfo) {
                        String[] list = nodeInfo.split(",");
                        boolean flag = false;
                        for (String s : list) {
                            if (Objects.equals(s, tableName) || Objects.equals(s.split("_")[0], tableName)) {
                                flag = true;
                                break;
                            }
                        }
                        if (flag) {
                            System.out.println("TARGET REGION SERVER: " + nodeInfo);
                            printWriter.println(nodeInfo);
                        }
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
