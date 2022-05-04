package com.largescalesystem.minisql.regionserver;
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
     * 获取mysql连接
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
