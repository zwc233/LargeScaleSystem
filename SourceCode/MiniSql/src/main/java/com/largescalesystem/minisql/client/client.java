package com.largescalesystem.minisql.client;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;

public class client {
    public static String getConnection(String ip, int port) throws IOException {
        String[] str = new String[100];
        //client cl = new client();
        Socket socket = null;
        InputStream inputStream = System.in;
        BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));

        OutputStream os = null;
//        Scanner sc = new Scanner(System.in);
//        String s = sc.nextLine();
//        for(int i = 0; i < s.length(); i++){
//            str[i] = String.valueOf(s.charAt(i));
//        }
        String info = null;
        str[0] = input.readLine();
        System.out.println(str[0]);
//        InetAddress serverIP = InetAddress.getByName(ip);
        socket = new Socket(ip, port);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        bw.write("CONNECT CLIENT");
        bw.newLine();
        bw.flush();
        int j = 0;
        while(str[j] != null) {
            BufferedReader br = null;
            try {
                PrintStream printStream = new PrintStream(socket.getOutputStream());
                printStream.println(str[j]);

                InputStream is = socket.getInputStream();
                br = new BufferedReader(new InputStreamReader(is));
                info = br.readLine();
                System.out.println(info);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try{
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (os != null) {
                    try{
                        os.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            j++;
        }

        return info;
    }

    public static class Communication extends Thread{
        private String ip;
        private int port;
        private String result;
        public Communication(String ip,int port){
            this.ip = ip;
            this.port = port;
        }

        public String getResult(){
            return this.result;
        }
        @Override
        public void run(){
            try {
                this.result = getConnection(ip, port);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String ip = "192.168.246.10";
        int port = 2000;
        //        String str1 = getConnection(ip,port);
        Communication communication2Master = new Communication(ip,port);
        communication2Master.start();
        communication2Master.join();
        String str1 = communication2Master.getResult();
        String str2 = null;
        String str3 = null;

//        String str1 = "127.0.0.1,1001,123,root,3306,3,school,student,teacher;127.0.0.1,1001,123,root,3306,3,school,student,teacher";
        String[] raw = str1.split(";");
        String region_ip = null;
        int region_port = 0;
        String mysql_port = null;
        String mysql_user = null;
        String mysql_pwd = null;
        int table_num = 0;
        String[] table = new String[100];

        String region_ip_slave = null;
        int region_port_slave = 0;
        String mysql_port_slave = null;
        String mysql_user_slave = null;
        String mysql_pwd_slave = null;
        int table_num_slave = 0;
        String[] table_slave = new String[100];


        String[] s = raw[0].split(",");
        System.out.println(raw[0]);
        region_ip = s[0];
        region_port = Integer.parseInt(s[1]);
        mysql_user = s[2];
        mysql_pwd = s[3];
        mysql_port = s[4];
        table_num = Integer.parseInt(s[5]);
        table = new String[100];
        for(int i = 0; i < table_num; i++){
            table[i] = s[i+6];
        }
        if(raw.length == 2){
            String[] s_slave = raw[1].split(",");
            region_ip_slave = s_slave[0];
            region_port_slave = Integer.parseInt(s_slave[1]);
            mysql_user_slave = s_slave[2];
            mysql_pwd_slave = s_slave[3];
            mysql_port_slave = s_slave[4];
            table_num_slave = Integer.parseInt(s_slave[5]);
            table_slave = new String[100];
            for(int i = 0; i < table_num; i++){
                table_slave[i] = s_slave[i+6];
            }
        }
        HashMap<String, Object> region_server_info = new HashMap<>();
        region_server_info.put("region_ip", region_ip);
        region_server_info.put("region_port", region_port);
        region_server_info.put("mysql_user", mysql_user);
        region_server_info.put("mysql_pwd", mysql_pwd);
        region_server_info.put("mysql_port", mysql_port);
        region_server_info.put("table_num", table_num);
        for (int i = 0; i < table_num; i++){
            String name = "table_name" + "_" + i;
            region_server_info.put(name,table[i]);
        }

        HashMap<String, Object> region_server_slave_info = new HashMap<>();
        region_server_slave_info.put("region_ip_slave", region_ip_slave);
        region_server_slave_info.put("region_port_slave", region_port_slave);
        region_server_slave_info.put("mysql_user_slave", mysql_user_slave);
        region_server_slave_info.put("mysql_pwd_slave", mysql_pwd_slave);
        region_server_slave_info.put("mysql_port_slave", mysql_port_slave);
        region_server_slave_info.put("table_num_slave", table_num_slave);
        for (int i = 0; i < table_num_slave; i++){
            String name_slave = "table_name_slave" + "_" + i;
            region_server_info.put(name_slave,table_slave[i]);
        }


        System.out.println("information of region_server is as follows");
        System.out.println(region_server_info);
        System.out.println("information of region_server_slave is as follows");
        System.out.println(region_server_slave_info);
//        System.out.println(region_ip);
//        System.out.println(region_port);

        while(true){
            if(raw.length == 1){
                Communication communication2Region = new Communication(region_ip, region_port);
                communication2Region.start();
                communication2Region.join();
                str2 = communication2Region.getResult();
                System.out.println(str2);
            }else{
                Communication communication2Region = new Communication(region_ip, region_port);
                communication2Region.start();
                communication2Region.join();
                str2 = communication2Region.getResult();
                System.out.println(str2);
                Communication communication2Region_slave = new Communication(region_ip_slave, region_port_slave);
                communication2Region_slave.start();
                communication2Region_slave.join();
                str3 = communication2Region_slave.getResult();
                System.out.println(str3);
            }
        }




    }


}