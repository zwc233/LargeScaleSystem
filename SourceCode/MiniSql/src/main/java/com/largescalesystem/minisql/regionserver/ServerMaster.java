package com.largescalesystem.minisql.regionserver;

import com.largescalesystem.minisql.zookeeper.ZooKeeperUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Objects;


public class ServerMaster {
    public static void createTable(String cmd){
        int index = cmd.indexOf("@");
        String sqlCmd = cmd.substring(0,index-1);
        String slaveInfoString = cmd.substring(index+2);
        String [] slaveInfo = slaveInfoString.split(",");

        String tableName = sqlCmd.split(" ")[2];
        tableName = tableName.substring(0,tableName.indexOf("("));
        System.out.println(sqlCmd);
        System.out.println(tableName);
        for (String i: slaveInfo) {
            System.out.println(i);
        }

        try {
            // 创建本地表
            RegionServer.statement.execute(sqlCmd);

            // 创建zookeeperNode
            String serverValue = addTable(Objects.requireNonNull(ZooKeeperUtils.getDataNode(RegionServer.client, RegionServer.serverPath)), tableName);
            ZooKeeperUtils.setDataNode(RegionServer.client, RegionServer.serverPath, serverValue);


            // 创建副本
            if (!slaveInfo[1].equals("127.0.0.1")) {
                // 创建远程表
                Connection connection = JdbcUtils.getConnection(slaveInfo[1],slaveInfo[3],slaveInfo[4]);
                assert connection != null;
                Statement statement =  connection.createStatement();
                statement.execute(sqlCmd);
                JdbcUtils.releaseResc(null,statement,connection);
                // 创建zookeeperNode
                String serverPath = "/lss/region_servers/" + slaveInfo[0];
                String remoteValue = addTable(Objects.requireNonNull(ZooKeeperUtils.getDataNode(RegionServer.client, serverPath)), tableName + "_slave");
                ZooKeeperUtils.setDataNode(RegionServer.client, serverPath, remoteValue);
            }

        } catch (Exception e) {
            System.out.println(e);
        }

    }

    public static void dumpTable(String cmd){
        int index = cmd.indexOf("@");
        String tableName = cmd.substring(cmd.indexOf(" " + 1),index - 1);
        String masterInfoString = cmd.substring(index+2);
        String [] masterInfo = masterInfoString.split(",");

        System.out.println(tableName);
        for (String i: masterInfo){
            System.out.println(i);
        }

        JdbcUtils.dumpRemoteSql(tableName, masterInfo[1], masterInfo[3], masterInfo[4]);
        File file = new File("./sql/lss." + tableName + ".sql");
        if (file.exists()){
            // 创建本地表
            String  str="mysql -u " + RegionServer.mysqlUser +
                    " -h " + "localhost" +
                    " -p" + RegionServer.mysqlPwd +
                    " < ./sql/lss." + tableName + ".sql";
            try {
                Runtime rt =Runtime.getRuntime();
                rt.exec("cmd /c " + str);
            } catch (IOException e){
                System.out.println(e);
                System.out.println(e.getStackTrace());
            }

            try {
                // 创建zookeeperNode
                String serverPath = RegionServer.serverPath;
                String remoteValue = addTable(Objects.requireNonNull(ZooKeeperUtils.getDataNode(RegionServer.client, serverPath)), tableName + "_slave");
                ZooKeeperUtils.setDataNode(RegionServer.client, serverPath, remoteValue);

                // 将原来副本升级为主表
                serverPath = "/lss/region_servers/" + masterInfo[0];
                String [] temp = ZooKeeperUtils.getDataNode(RegionServer.client, serverPath).split(",");

                for (int i = 6;i<temp.length;i++){
                    if (temp[i].equals(tableName + "_slave")){
                        temp[i] = tableName;
                    }
                }

                StringBuilder newValue = new StringBuilder();
                for (String i: temp){
                    newValue.append(i).append(",");
                }
                ZooKeeperUtils.setDataNode(RegionServer.client, serverPath, newValue.substring(0,newValue.toString().length()-1));

            } catch (Exception e) {
                System.out.println(e);
            }
        } else {
            System.out.println("创建副本失败");
        }
    }

    public static void migrateTable(String cmd){
        int index = cmd.indexOf("@");
        String tableName = cmd.substring(cmd.indexOf(" " + 1),index - 1);
        String masterInfoString = cmd.substring(index+2);
        String [] masterInfo = masterInfoString.split(",");

        JdbcUtils.dumpRemoteSql(tableName, masterInfo[1], masterInfo[3], masterInfo[4]);
        File file = new File("./sql/lss." + tableName + ".sql");
        if (file.exists()){
            // 创建本地表
            String  str="mysql -u " + RegionServer.mysqlUser +
                    " -h " + "localhost" +
                    " -p" + RegionServer.mysqlPwd +
                    " < ./sql/lss." + tableName + ".sql";
            try {
                Runtime rt =Runtime.getRuntime();
                rt.exec("cmd /c " + str);
            } catch (IOException e){
                System.out.println(e);
                System.out.println(e.getStackTrace());
            }

            try {
                // 创建zookeeperNode
                String serverPath = RegionServer.serverPath;
                String remoteValue = addTable(Objects.requireNonNull(ZooKeeperUtils.getDataNode(RegionServer.client, serverPath)), tableName);
                ZooKeeperUtils.setDataNode(RegionServer.client, serverPath, remoteValue);

                // 将原来表删除
                // 删除本地表
                Connection connection = JdbcUtils.getConnection(masterInfo[1],masterInfo[3],masterInfo[4]);
                assert connection != null;
                Statement statement =  connection.createStatement();
                statement.execute("drop table if exists "+tableName);

                JdbcUtils.releaseResc(null,statement,connection);

                // 创建zookeeperNode
                serverPath = "/lss/region_servers/" + masterInfo[0];
                remoteValue = deleteTable(Objects.requireNonNull(ZooKeeperUtils.getDataNode(RegionServer.client, serverPath)), tableName + "_slave");
                ZooKeeperUtils.setDataNode(RegionServer.client, serverPath, remoteValue);

            } catch (Exception e) {
                System.out.println(e);
            }




            
        } else {
            System.out.println("创建副本失败");
        }
    }
    public static String deleteTable(String serverValue, String tableName) {
        String [] temp = serverValue.split(",");
        temp[5] = String.valueOf(Integer.parseInt(temp[5]) - 1);
        StringBuilder newValue = new StringBuilder();

        for (String i: temp){
            if (!i.equals(tableName))
                newValue.append(i).append(",");
        }
        return newValue.substring(0,newValue.toString().length()-1);
    }
    public static String addTable(String serverValue, String tableName){
        String [] temp = serverValue.split(",");
        temp[5] = String.valueOf(Integer.parseInt(temp[5]) + 1);
        StringBuilder newValue = new StringBuilder();
        for (String i: temp){
            newValue.append(i).append(",");
        }
        return newValue + tableName;
    }
}
