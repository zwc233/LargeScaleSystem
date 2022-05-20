package com.largescalesystem.minisql.client;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ClientTest {
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
//        System.out.println(str[0]);
        InetAddress serverIP = InetAddress.getByName(ip);
        socket = new Socket(serverIP, port);
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
//                System.out.println(info);
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

    public static void main(String[] args) throws IOException {
        String ip = "127.0.0.1";
        int port = 2000;
        String str1 = getConnection(ip,port);
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
        region_ip = s[0];
        region_port = Integer.parseInt(s[1]);
        mysql_port = s[4];
        mysql_user = s[2];
        mysql_pwd = s[3];
        table_num = Integer.parseInt(s[5]);
        table = new String[100];
        for(int i = 0; i < table_num; i++){
            table[i] = s[i+6];
        }
        if(raw.length == 2){
            String[] s_slave = raw[1].split(",");
            region_ip_slave = s_slave[0];
            region_port_slave = Integer.parseInt(s_slave[1]);
            mysql_port_slave = s_slave[4];
            mysql_user_slave = s_slave[2];
            mysql_pwd_slave = s_slave[3];
            table_num_slave = Integer.parseInt(s_slave[5]);
            table_slave = new String[100];
            for(int i = 0; i < table_num; i++){
                table_slave[i] = s_slave[i+6];
            }
        }

        System.out.println("REGION IP: " + region_ip + ", PORT: " + region_port);
//        System.out.println(region_ip_slave);

        String str2 = getConnection(region_ip,region_port);
    }


}
