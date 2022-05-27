package com.largescalesystem.minisql.regionserver;


import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;

import org.apache.curator.framework.CuratorFramework;

public class ServerThread implements Runnable {
    public Socket socket;
    public String serverIdentity = null;
    public BufferedReader reader = null;
    public CuratorFramework client = null;
    public ArrayList<TableInfo> tables;
    public Statement statement;

    public BufferedWriter writer = null;


    ServerThread(Socket socket, CuratorFramework client, Statement statement, ArrayList<TableInfo> tables) {
        this.socket = socket;
        this.client = client;
        this.tables = tables;
        this.statement = statement;
    }
    @Override
    public void run() {
        try {
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            String line;


            while ((line = reader.readLine()) != null){
                if (line.substring(0, 7).equals("CONNECT")){
                    serverIdentity = line.substring(8);
                    System.out.println(serverIdentity + "已连接");
                    break;
                }
                if(line.equals("exit")){
                    System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                    break;
                }
            }

            if (serverIdentity != null) {
                if (serverIdentity.equals("CLIENT")){
                    serveClient();
                } else if (serverIdentity.equals("MASTER")) {
                    serveMaster();
                } else if (serverIdentity.equals("REGION SERVER")) {
                    serveRegionServer();
                } else {
                    System.out.println("未知连接");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void serveClient() throws IOException{
        String line;
        while ((line = reader.readLine()) != null){
            System.out.println("Client服务");
            if(line.equals("exit")){
                System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                break;
            } else if (line.length() >= 6 && line.startsWith("create")){
                System.out.println("Client请求创建表");
                synchronized (this){
                    boolean res = ServerClient.createTable(line);
                    reactCmd(res);
                }
            } else if (line.length() >= 6 && line.startsWith("insert")){
                System.out.println("Client请求插入语句");
                synchronized (this){
                    boolean res = ServerClient.executeCmd(line);
                    reactCmd(res);
                }
            } else if (line.length() >= 6 && line.startsWith("delete")){
                System.out.println("Client请求删除语句");
                synchronized (this){
                    boolean res = ServerClient.executeCmd(line);
                    reactCmd(res);
                }
            } else if (line.length() >= 6 && line.startsWith("select")){
                System.out.println("Client请求查询");
                synchronized (this){
                    String res = ServerClient.selectTable(line);
                    writer.write(res + "\n");
                    writer.flush();
                }
            } else if (line.length() >= 4 && line.startsWith("drop")){
                System.out.println("Client请求删除表");
                synchronized (this){
                    boolean res = ServerClient.dropTable(line);
                    reactCmd(res);
                }
            } else {
                System.out.println("非有效指令 " + line);
            }
        }
    }
    public void serveMaster() throws IOException{
        String line;
        while ((line = reader.readLine()) != null){
            System.out.println("Master服务");
            if(line.equals("exit")){
                System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                break;
            } else if (line.length() >= 7 && line.startsWith("migrate")){
                System.out.println("Master请求迁移表");
                synchronized (this){
                    ServerMaster.migrateTable(line);
                }
            } else if (line.length() >= 4 && line.startsWith("dump")){
                System.out.println("Master请求创建副本");
                synchronized (this){
                    ServerMaster.dumpTable(line);
                }
            } else if (line.startsWith("create")){
                System.out.println("Master请求创建表");
            } else {
                System.out.println("非有效指令 " + line);
            }
        }
    }
    public void serveRegionServer() throws IOException{
        String line;
        while ((line = reader.readLine()) != null){
            System.out.println("RegionServer服务");
            if(line.equals("exit")){
                System.out.println(socket.getInetAddress().getHostAddress() + "正在退出连接！");
                break;
            } else if (line.length() >= 6 && line.startsWith("create")){
                System.out.println("RegionServer同步请求创建表");
            } else if (line.length() >= 6 && line.startsWith("insert")){
                System.out.println("RegionServer同步请求插入语句");
            } else if (line.length() >= 6 && line.startsWith("delete")){
                System.out.println("RegionServer同步请求删除语句");
            } else if (line.length() >= 4 && line.startsWith("dump")){
                System.out.println("RegionServer同步请求创建数据副本");
            } else if (line.length() >= 4 && line.startsWith("drop")){
                System.out.println("RegionServer同步请求删除表");
            } else {
                System.out.println("非有效指令 " + line);
            }
        }
    }

    public void reactCmd(boolean result) throws IOException {
        if (result){
            writer.write("true\n");
        } else {
            writer.write("false\n");
        }
        writer.flush();
    }
}