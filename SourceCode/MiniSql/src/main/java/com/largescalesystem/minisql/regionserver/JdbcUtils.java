package com.largescalesystem.minisql.regionserver;
import java.io.IOException;
import java.sql.*;

public class JdbcUtils {

    private static final String driver;
    private static final String url;
    private static final String user;
    private static final String pwd;

    static{
        driver = "com.mysql.cj.jdbc.Driver";
        url = "jdbc:mysql://localhost:3306/?characterEncoding=utf-8&"
                + "useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
        user = "root";
        pwd = "123";
    }

    /**
     * 获取mysql连接，写死url，name，pwd用于测试
     */
    static public Connection getConnection(){
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url,user,pwd);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 支持自定义用户名
     * @param inputUser mysql用户名
     * @param inputPwd mysql密码
     */
    static public Connection getConnection(String inputUser, String inputPwd){
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url,inputUser,inputPwd);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 支持远程连接
     * @param inputIP 远程IP
     * @param inputUser mysql用户名
     * @param inputPwd mysql密码
     */
    static public Connection getConnection(String inputIP ,String inputUser, String inputPwd){
        try {
            Class.forName(driver);
            String inputURl = "jdbc:mysql://" + inputIP + "/?characterEncoding=utf-8&"
                    + "useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
            return DriverManager.getConnection(inputURl,inputUser,inputPwd);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    

    /**
     * 数据表dump
     * @param inputTable 需要dump的表，默认存在lss数据库下
     */
    public static boolean dumpRemoteSql(String inputTable) {
        String  str="mysqldump -uroot -p123 lss " + inputTable + 
        " > ./sql/lss." + inputTable + ".sql";
        
        try {
            Runtime rt =Runtime.getRuntime();
            rt.exec("cmd /c " + str);
        } catch (IOException e){
            System.out.println(e);
            System.out.println(e.getStackTrace());
        }
        return true;
    }

    /**
     * 数据表dump
     * @param inputTable 需要dump的表，默认存在lss数据库下
     * @param ip 远程IP
     */
    public static boolean dumpRemoteSql(String inputTable, String ip) {
        String  str="mysqldump -uroot -h" + ip + 
        " -p123 lss " + inputTable + 
        " > ./sql/lss." + inputTable + ".sql";

        try {
            Runtime rt =Runtime.getRuntime();
            rt.exec("cmd /c " + str);
        } catch (IOException e){
            System.out.println(e);
            System.out.println(e.getStackTrace());
        }
        return true;
    }

    /**
     * 数据表dump
     * @param inputTable 需要dump的表，默认存在lss数据库下
     * @param ip 远程IP
     * @param user mysql用户名
     * @param pwd mysql密码
     */
    public static boolean dumpRemoteSql(String inputTable, String ip, String user, String pwd) {
        // TODO 在dump之前检查 ip连接是否通畅
        String  str="mysqldump -u" + user + 
        " -h" + ip + 
        " -p" + pwd +
        " lss " + inputTable + 
        " > ./sql/lss." + inputTable + ".sql";
        try {
            Runtime rt =Runtime.getRuntime();
            rt.exec("cmd /c " + str);
        } catch (IOException e){
            System.out.println(e);
            System.out.println(e.getStackTrace());
        }
        return true;
    }

    /**
     * 释放Mysql资源
     * @param resultSet 结果集
     * @param statement 状态
     * @param connection 链接
     */
    public static void releaseResc(ResultSet resultSet, Statement statement, Connection connection) {
        try {
            if(resultSet!=null){
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if(statement!=null){
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if(connection!=null){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
