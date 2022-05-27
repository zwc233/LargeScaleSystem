package com.largescalesystem.minisql;

import com.largescalesystem.minisql.regionserver.JdbcUtils;
import org.junit.Test;
import java.sql.*;

public class MysqlTest {
    @Test
    public void jdbcQuery(){

        ResultSet resultSet = null;
        Statement statement = null;
        Connection connection = null;
        try {
            connection = JdbcUtils.getConnection();
            statement = connection.createStatement();
            String dbSql = "use school";
            statement.executeQuery(dbSql);

            String sql = "select id,teacher_name from student";

            resultSet = statement.executeQuery(sql);
            ResultSetMetaData rsmd = resultSet.getMetaData() ; 
            int columnCount = rsmd.getColumnCount();
            String res = "";
            while(resultSet.next()){
                if (columnCount >= 1)
                    res +=  resultSet.getString(1);
                for (int i = 2; i<= columnCount;i++){
                    res +=  " " + resultSet.getString(i);
                }
                res += "\n";
            }
            System.out.println(res);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            JdbcUtils.releaseResc(resultSet, statement, connection);
        }
    }

    @Test
    public void mySqlDump(){
        JdbcUtils.dumpRemoteSql("course", "127.0.0.1", "root", "123");
    }
}
