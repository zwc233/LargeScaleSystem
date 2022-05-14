package com.largescalesystem.minisql;
import org.junit.Test;
import org.apache.log4j.Logger;
public class LogTest {
    @Test
    public void logTest(){
        System.out.println("hello world");
        Logger logger = Logger.getLogger(LogTest.class);
        logger.info("info:主函数运行");
        logger.debug("Debug:主函数运行");
        logger.error("error:主函数运行");
    }
    @Test
    public void testPrint(){
        System.out.println("hello world");
    }
}
