package com.largescalesystem.minisql.regionserver;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
public class Client {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 1001);

        // socket 获取字符流
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        bufferedWriter.write("CONNECT CLIENT");
        bufferedWriter.newLine();
        bufferedWriter.flush();
        //通过标准输入流获取字符流
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while((line = bufferedReader.readLine()) != null){
            bufferedWriter.write(line);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        }
    }
}
