package com.largescalesystem.minisql;
import com.largescalesystem.minisql.regionserver.RegionServer;

import org.junit.Test;

public class RegionServerTest {
    @Test
    public void testIPGenerate() {
        RegionServer.getIPAddress();
    }

    @Test
    public void testPortGenerate() {
        System.out.println(RegionServer.getAvailableTcpPort());
    }  
}
