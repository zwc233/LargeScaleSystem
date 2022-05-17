package com.largescalesystem.minisql.regionserver;

import java.sql.*;

public class ServerClient {
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

    public static boolean insertTable(String cmd){
        try {
            return RegionServer.statement.execute(cmd);
        } catch (Exception e){
            System.out.println(e);
        }
        return false;
    }
}
