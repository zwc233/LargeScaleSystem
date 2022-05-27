package com.largescalesystem.minisql.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Objects;

import static com.largescalesystem.minisql.master.RegionManager.listenNode;

public class Server {
    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(2000);
        System.out.println("Master is waiting for connection");
        for(;;){
            Socket client = server.accept();
            ClientHandler clientHandler = new ClientHandler(client);
            clientHandler.start();
            listenNode();
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
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String temp = input.readLine();
                String sql = input.readLine();
                System.out.println("CLIENT REQUEST: " + sql);
                ArrayList<String> tableInfo = RegionManager.getNodeList();
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                String tableName = null;
                if (Objects.equals(sql.split(" ")[0], "insert") || Objects.equals(sql.split(" ")[0], "delete") ||
                        Objects.equals(sql.split(" ")[0], "drop")) {
                    tableName = sql.split(" ")[2];
                }
                else if(Objects.equals(sql.split(" ")[0], "select")){
                    tableName = sql.split(" ")[3];
                }
                else if(Objects.equals(sql.split(" ")[0], "create")){
                    tableName = sql.split("\\(")[0].split(" ")[2];
                }
                if(Objects.equals(sql.split(" ")[0], "create")){
                    System.out.println("TARGET REGION SERVER: " + RegionManager.loadBalance(tableInfo));
                    printWriter.println(RegionManager.loadBalance(tableInfo));
                }
                else {
                    StringBuilder result = new StringBuilder();
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
                            if(result.toString().equals("")){
                                result.append(nodeInfo).append(";");
                            }
                            else{
                                result.append(nodeInfo);
                            }
                        }
                    }
                    printWriter.println(result);
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
