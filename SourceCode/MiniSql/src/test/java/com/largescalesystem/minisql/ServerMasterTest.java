package com.largescalesystem.minisql;
import com.largescalesystem.minisql.regionserver.ServerMaster;
import org.junit.Test;
public class ServerMasterTest {
    @Test
    public void createTableTest(){
        ServerMaster.createTable("create table school(name char(20), id int) @ 127.0.0.1,root,123");
    }
    @Test
    public void testPrint(){
        System.out.println("hello world");
    }
}
