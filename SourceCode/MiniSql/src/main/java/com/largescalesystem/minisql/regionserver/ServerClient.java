package com.largescalesystem.minisql.regionserver;

import com.largescalesystem.minisql.zookeeper.ZooKeeperUtils;

import java.sql.*;
import java.util.Objects;

public class ServerClient {
    public static boolean createTable(String sqlCmd){
//        int index = cmd.indexOf("@");
//        String sqlCmd = cmd.substring(0,index-1);
//        String slaveInfoString = cmd.substring(index+2);
//        String [] slaveInfo = slaveInfoString.split(",");

        String tableName = sqlCmd.split(" ")[2];
        tableName = tableName.substring(0,tableName.indexOf("("));
        System.out.println(sqlCmd);
        System.out.println(tableName);
//        for (String i: slaveInfo) {
//            System.out.println(i);
//        }

        try {
            // 创建本地表
            RegionServer.statement.execute(sqlCmd);
            // 创建zookeeperNode
            String serverValue = ServerMaster.addTable(Objects.requireNonNull(ZooKeeperUtils.getDataNode(RegionServer.client, RegionServer.serverPath)), tableName);
            System.out.println(serverValue);
            ZooKeeperUtils.setDataNode(RegionServer.client, RegionServer.serverPath, serverValue);
            return true;
        } catch (Exception e) {
            System.out.println(e);
        }
        return false;
    }

    public static boolean dropTable(String cmd){
        String tableName = cmd.split(" ")[2];
        try {
            // 创建本地表
            RegionServer.statement.execute(cmd);
            // 创建zookeeperNode
            String serverValue = ServerMaster.deleteTable(Objects.requireNonNull(ZooKeeperUtils.getDataNode(RegionServer.client, RegionServer.serverPath)), tableName);
            System.out.println(serverValue);
            ZooKeeperUtils.setDataNode(RegionServer.client, RegionServer.serverPath, serverValue);
            return true;
        } catch (Exception e) {
            System.out.println(e);
        }
        return false;
    }

    public static String selectTable(String cmd){
        String res = "";
        try {
            ResultSet r =  RegionServer.statement.executeQuery(cmd);
            ResultSetMetaData rsmd = r.getMetaData(); 
            int columnCount = rsmd.getColumnCount();
            while(r.next()){
                if (columnCount >= 1)
                res +=  r.getString(1);
                for (int i = 2; i<= columnCount;i++){
                    res +=  " " + r.getString(i);
                }
                res += "\n";
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return res;
    }

    public static boolean executeCmd(String cmd){
        try {
            RegionServer.statement.execute(cmd);
            return true;
        } catch (Exception e){
            System.out.println(e);
        }
        return false;
    }

}
