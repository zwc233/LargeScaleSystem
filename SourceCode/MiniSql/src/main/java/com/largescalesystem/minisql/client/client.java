package com.largescalesystem.minisql.client;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Objects;
import java.util.Scanner;

public class client {
    public static String getConnection(String sql, String ip, int port) throws IOException {
        Socket socket = null;
        OutputStream os = null;
        String info = null;
        socket = new Socket(ip, port);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        bw.write("CONNECT CLIENT");
        bw.newLine();
        bw.flush();
        int j = 0;
        BufferedReader br = null;
        try {
//            PrintStream printStream = new PrintStream(socket.getOutputStream());
//            printStream.println(sql);
            bw.write(sql);
            bw.newLine();
            bw.flush();
            InputStream is = socket.getInputStream();
            br = new BufferedReader(new InputStreamReader(is));
            info = br.readLine();
            System.out.println(info);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(bw != null){
                try{
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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


        return info;
    }

    public static class Communication extends Thread{
        private String sql;
        private String ip;
        private int port;
        private String result;
        public Communication(String sql,String ip,int port){
            this.sql = sql;
            this.ip = ip;
            this.port = port;

        }

        public String getResult(){
            return this.result;
        }
        @Override
        public void run(){
            try {
                this.result = getConnection(sql, ip, port);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        while(true) {
            String ip = "127.0.0.1";
//            String ip = "127.0.0.1";
            int port = 2000;
            String sql = null;
            Scanner sc = new Scanner(System.in);
            sql = sc.nextLine();
            while(sql != "quit") {

            client.Communication communication2Master = new client.Communication(sql, ip, port);
            communication2Master.start();
            communication2Master.join();
            String str1 = communication2Master.getResult();
            String str2 = null;
            String str3 = null;

//      String str1 = "127.0.0.1,1001,123,root,3306,3,school,student,teacher;127.0.0.1,1001,123,root,3306,3,school,student,teacher";
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
            for (int i = 0; i < table_num; i++) {
                table[i] = s[i + 6];
            }
            if (raw.length == 2) {
                String[] s_slave = raw[1].split(",");
                region_ip_slave = s_slave[0];
                region_port_slave = Integer.parseInt(s_slave[1]);
                mysql_user_slave = s_slave[2];
                mysql_pwd_slave = s_slave[3];
                mysql_port_slave = s_slave[4];
                table_num_slave = Integer.parseInt(s_slave[5]);
                table_slave = new String[100];
                for (int i = 0; i < table_num; i++) {
                    table_slave[i] = s_slave[i + 6];
                }
            }
            HashMap<String, Object> region_server_info = new HashMap<>();
            region_server_info.put("region_ip", region_ip);
            region_server_info.put("region_port", region_port);
            region_server_info.put("mysql_user", mysql_user);
            region_server_info.put("mysql_pwd", mysql_pwd);
            region_server_info.put("mysql_port", mysql_port);
            region_server_info.put("table_num", table_num);
            for (int i = 0; i < table_num; i++) {
                String name = "table_name" + "_" + i;
                region_server_info.put(name, table[i]);
            }

            HashMap<String, Object> region_server_slave_info = new HashMap<>();
            region_server_slave_info.put("region_ip_slave", region_ip_slave);
            region_server_slave_info.put("region_port_slave", region_port_slave);
            region_server_slave_info.put("mysql_user_slave", mysql_user_slave);
            region_server_slave_info.put("mysql_pwd_slave", mysql_pwd_slave);
            region_server_slave_info.put("mysql_port_slave", mysql_port_slave);
            region_server_slave_info.put("table_num_slave", table_num_slave);
            for (int i = 0; i < table_num_slave; i++) {
                String name_slave = "table_name_slave" + "_" + i;
                region_server_info.put(name_slave, table_slave[i]);
            }


            System.out.println("information of region_server is as follows");
            System.out.println(region_server_info);
            System.out.println("information of region_server_slave is as follows");
            System.out.println(region_server_slave_info);
//            sc.close(); //会出现noline found
//            System.out.println(region_ip);
//            System.out.println(region_port);
            String[] temp = sql.split(" ");
            String sql_slave = null;
            int index;
            if(Objects.equals(temp[0], "create")){
                index = temp[2].indexOf("(");
                StringBuffer stringBuffer = new StringBuffer(temp[2]);
                stringBuffer.insert(index,"_slave");
                temp[2] = stringBuffer.toString();
                StringBuffer sb = new StringBuffer();
                for(int i = 0;i < temp.length-1;i++){
                    sb.append(temp[i]+" ");
                }
                sb.append(temp[temp.length - 1]);
                sql_slave = sb.toString();
            }else if(Objects.equals(temp[0], "insert")){
                StringBuffer stringBuffer = new StringBuffer(temp[2]);
                stringBuffer.append("_slave");
                temp[2] = stringBuffer.toString();
                StringBuffer sb = new StringBuffer();
                for(int i = 0;i < temp.length-1;i++){
                    sb.append(temp[i]+" ");
                }
                sb.append(temp[temp.length - 1]);
                sql_slave = sb.toString();
            }else if(Objects.equals(temp[0],"drop")){
                StringBuffer stringBuffer = new StringBuffer(temp[2]);
                stringBuffer.append("_slave");
                temp[2] = stringBuffer.toString();
                sql_slave = temp[0] + " " + temp[1] + " " + temp[2];
            }
            System.out.println(raw.length);
            System.out.println(sql_slave);

            //若无法跟region连接，这段代码会报错connect refused
//            if (raw.length == 1) {
//                client.Communication communication2Region = new client.Communication(sql, region_ip, region_port);
//                communication2Region.start();
//                communication2Region.join();
//                str2 = communication2Region.getResult();
//                System.out.println(str2);
//            } else {
//                client.Communication communication2Region = new client.Communication(sql, region_ip, region_port);
//                communication2Region.start();
//                communication2Region.join();
//                str2 = communication2Region.getResult();
//                System.out.println(str2);
//                client.Communication communication2Region_slave = new client.Communication(sql_slave, region_ip_slave, region_port_slave);
//                communication2Region_slave.start();
//                communication2Region_slave.join();
//                str3 = communication2Region_slave.getResult();
//                System.out.println(str3);
//            }

            sql = null;
            break;
            }
        }

    }
}