package com.largescalesystem.minisql.regionserver;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.sql.*;

import org.apache.curator.framework.CuratorFramework;

public class ServerThread implements Runnable {
    public Socket socket;
    public String serverIdentity = null;
    public BufferedReader reader = null;
    public CuratorFramework client = null; 

    ServerThread(Socket socket, CuratorFramework client, Statement statement) {
        this.socket = socket;
        this.client = client;
    }
    @Override
    public void run() {
        try {
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
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
            } else if (line.length() >= 6 && line.substring(0, 6).equals("create")){
                System.out.println("Client请求创建表");
            } else if (line.length() >= 6 && line.substring(0, 6).equals("insert")){
                System.out.println("Client请求插入语句");
            } else if (line.length() >= 6 && line.substring(0, 6).equals("delete")){
                System.out.println("Client请求删除语句");
            } else if (line.length() >= 6 && line.substring(0, 6).equals("select")){
                System.out.println("Client请求查询");
            } else if (line.length() >= 4 && line.substring(0, 4).equals("drop")){
                System.out.println("Client请求删除表");
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
            } else if (line.length() >= 7 && line.substring(0, 7).equals("migrate")){
                System.out.println("Master请求迁移表");
            } else if (line.length() >= 4 && line.substring(0, 4).equals("dump")){
                System.out.println("Master请求创建副本");
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
            } else if (line.length() >= 6 && line.substring(0, 6).equals("create")){
                System.out.println("RegionServer同步请求创建表");
            } else if (line.length() >= 6 && line.substring(0, 6).equals("insert")){
                System.out.println("RegionServer同步请求插入语句");
            } else if (line.length() >= 6 && line.substring(0, 6).equals("delete")){
                System.out.println("RegionServer同步请求删除语句");
            } else if (line.length() >= 4 && line.substring(0, 4).equals("dump")){
                System.out.println("RegionServer同步请求创建数据副本");
            } else if (line.length() >= 4 && line.substring(0, 4).equals("drop")){
                System.out.println("RegionServer同步请求删除表");
            } else {
                System.out.println("非有效指令 " + line);
            }
        }
    }
}