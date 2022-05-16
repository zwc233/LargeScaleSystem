package com.largescalesystem.minisql.master;

import org.apache.curator.framework.CuratorFramework;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Master {
    public static String ip = null;
    public static int port;
    public static CuratorFramework client = null;

    static {
        port = 2000;
    }

    public static void main(String[] args) throws IOException{
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(Inet4Address.getLocalHost(), port));
        InputStream in = System.in;
        BufferedReader input = new BufferedReader(new InputStreamReader(in));

        try{
            masterHandle(socket);
        }catch (Exception e){
            System.out.println("Master Error");
        }
    }

    public static void masterHandle(Socket socket) throws IOException{
        InputStream in = System.in;
        BufferedReader input = new BufferedReader(new InputStreamReader(in));
        OutputStream outputStream = socket.getOutputStream();
        PrintStream socketPrintStream = new PrintStream(outputStream);

        InputStream  inputStream = socket.getInputStream();
        BufferedReader socketBufferReader = new BufferedReader(new InputStreamReader(inputStream));

        boolean flag = true;
        do{
            String str = input.readLine();
            socketPrintStream.println(str);
            String echo = socketBufferReader.readLine();
            if("exit".equalsIgnoreCase(echo)){
                flag = false;
            }
        } while (flag);

        socketPrintStream.close();
        socketBufferReader.close();
    }

}
