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

            String sql = "select * from student";

            resultSet = statement.executeQuery(sql);

            while(resultSet.next()){

                String id = resultSet.getString("id");
                String name = resultSet.getString("name");
                String teacherName = resultSet.getString("teacher_name");

                System.out.println("学号:"+id+"  姓名:"+name+"  教师姓名:"+teacherName);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            JdbcUtils.releaseResc(resultSet, statement, connection);
        }
    }
}
