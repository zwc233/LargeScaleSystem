package com.largescalesystem.minisql.master;

import com.largescalesystem.minisql.regionserver.RegionServer;
import com.largescalesystem.minisql.regionserver.ServerMaster;
import com.largescalesystem.minisql.zookeeper.ZooKeeperUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RegionManager{
    public static CuratorFramework client = null;
    public static String address = "192.168.246.136:2181";
    public static ArrayList<String> nodeList = null;
    public static String masterInfo = null;
    public static List<String> regions = null;

    public static void getRegionAndMaster() {
        try{
            regions = client.getChildren().forPath("/lss/region_servers");
            for(String region : regions){
                System.out.println(ZooKeeperUtils.getDataNode(client, "/lss/region_servers/" + region));
                nodeList.add(ZooKeeperUtils.getDataNode(client, "/lss/region_servers/" + region));
            }
            masterInfo = ZooKeeperUtils.getDataNode(client, "/lss/master");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<String> getNodeList() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder().connectString(address).sessionTimeoutMs(3000)
                .connectionTimeoutMs(3000).retryPolicy(retryPolicy).build();
        client.start();
        nodeList = new ArrayList<>();
        getRegionAndMaster();
        nodeCuratorCache(client, "/lss/region_servers");
        return nodeList;
    }

    /**
     * 监听某个节点
     * @param client
     * @param path
     */
    public static void nodeCuratorCache(CuratorFramework client, String path) {
        CuratorCache curatorCache = CuratorCache.build(client,path);
        CuratorCache cache = CuratorCache.build(client, path);
        CuratorCacheListener listener = CuratorCacheListener.builder()
                .forCreates(node -> System.out.printf("Node created: [%s]%n", node))
                .forChanges((oldNode, node) -> System.out.printf("Node changed. Old: [%s] New: [%s]%n", oldNode, node))
                .forDeletes(oldNode -> System.out.printf("Node deleted. Old value: [%s]%n", oldNode))
                .forInitialized(() -> System.out.println("Cache initialized"))
                .build();

        cache.listenable().addListener(listener);

        cache.start();
    }

    public static String loadBalance(ArrayList<String> nodeInfo){
        if(nodeInfo.size() == 1){
            return nodeInfo.get(0);
        }
        else {
            ArrayList<String[]> node = new ArrayList<>();
            int min1 = 0;
            for (String s : nodeInfo) {
                node.add(s.split(","));
                if (s.split(",").length > min1) {
                    min1 = s.split(",").length;
                }
            }
            int min2 = min1;
            int index1 = 0, index2 = 0;
            for (String[] arr : node) {
                if (arr.length <= min2) {
                    if (arr.length >= min1) {
                        min2 = arr.length;
                        index2 = node.indexOf(arr);
                    } else {
                        min2 = min1;
                        min1 = arr.length;
                        index2 = index1;
                        index1 = node.indexOf(arr);
                    }
                }
            }
            System.out.println(index1 + " " + index2);
            return nodeInfo.get(index1) + ";" + nodeInfo.get(index2);
        }
    }

    public static void listenNode() throws Exception {
        String brokenRegion;
        // 重新获取当前活跃节点
        List<String> regionsNew = client.getChildren().forPath("/lss/region_servers");
        try{
            for(String region : regions){
                // 发现某一节点宕机
                if(!regionsNew.contains(region)){
                    brokenRegion = region;
                    // 获取节点下标
                    int index = regions.indexOf(brokenRegion);
                    // 获取对应的宕机节点的信息
                    String[] nodeInfo = nodeList.get(index).split(",");
                    // 更新nodelist
                    nodeList.remove(index);
                    // 获取节点的表的信息
                    ArrayList<String> tableList = new ArrayList<>(Arrays.asList(nodeInfo).subList(6, nodeInfo.length));
                    // 对每一个表
                    for(String table : tableList){
                        // 如果是副表
                        if(Objects.equals(table.split("_")[table.split("_").length - 1], "slave")){
                            StringBuilder tableName = new StringBuilder();
                            // 获取对应的主表的名字
                            for(int i = 0; i < table.split("_").length - 1; i++){
                                tableName.append(table.split("_")[i]);
                            }
                            // 获取不包含该主表或对应副表的其他节点中含有表最少的节点
                            int indexList = updateSlave(table);
                            // 获取主表存放的server
                            String mainServer = null;
                            for(String info : nodeList){
                                String[] tmp = info.split(",");
                                ArrayList<String> tmpTable = new ArrayList<>(Arrays.asList(tmp).subList(6, tmp.length));
                                if(tmpTable.contains(tableName.toString())){
                                    mainServer = info;
                                    break;
                                }
                            }
                            // 在该节点复制对应的表
                            if(indexList != -1){
                                // 获取目标节点的ip和port
                                nodeInfo = nodeList.get(indexList).split(",");
                                // 获取主表节点的ip和port
                                String[] mainList = mainServer.split(",");
                                String cmd = "dump " + tableName + " @ " + regions.get(nodeList.indexOf(mainServer)) + "," + mainList[0] + "," + mainList[1]
                                        + ",root,123";
                                Socket socket = new Socket();
                                socket.connect(new InetSocketAddress(nodeInfo[0], Integer.parseInt(nodeInfo[1])), 2000);
                                try{
                                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                                    printWriter.println("MASTER CONNECTED");
                                    printWriter.println(cmd);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                socket.close();
                            }
                        }
                        // 如果是主表
                        else {
                            // 获取创建主表的region
                            int indexMain = updateSlave(table);
                            // 可以创建主表
                            if(indexMain != -1){
                                nodeInfo = nodeList.get(indexMain).split(",");
                                String cmd = "migrate " + table + " @ " + regions.get(indexMain) + "," + nodeInfo[0] + "," + nodeInfo[1]
                                        + ",root,123";
                            }
                            // 全部节点存在副本
                            else{
                                // 找到一个节点升级为主表
                                for(String node: nodeList){
                                    ArrayList<String> tmp = new ArrayList<>(Arrays.asList(node.split(",")).subList(6, node.split(",").length));
                                    for(String tableTmp : tmp) {
                                        // 某一个节点中存在一个副本
                                        StringBuilder mainTable = new StringBuilder();
                                        String[] tableSplit = tableTmp.split("_");
                                        for (int i = 0; i < tableSplit.length - 1; i++) {
                                            mainTable.append(tableSplit[i]);
                                            if (i < tableSplit.length - 2) {
                                                mainTable.append("_");
                                            }
                                        }
                                        if(mainTable.toString().equals(table)){

                                        }
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static int updateSlave(String table){
        StringBuilder tableName = new StringBuilder();
        String[] op = table.split("_");
        //获取主表名称
        for(int i = 0; i < op.length - 1; i++){
            tableName.append(op[i]);
            if(i < op.length - 2){
                tableName.append("_");
            }
        }
        int min = 100;
        int index = -1;
        for(String node : nodeList){
            // 有节点不包含主表或者副表
            if(!(Arrays.asList(node.split(",")).contains(tableName.toString())
                    || Arrays.asList(node.split(",")).contains(table))){
                // 获取表最少的节点
                if(node.split(",").length < min){
                    min = node.split(",").length;
                    index = nodeList.indexOf(node);
                }
            }
        }
        // 返回最少的表的节点的index
        return index;
    }

    public static class handleMistake extends Thread {
        private final Socket socket;
        private String cmd;

        handleMistake(Socket socket, String cmd){
            this.socket = socket;
            this.cmd = cmd;
        }

        @Override
        public void run(){
            super.run();
            try{
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                printWriter.println("MASTER CONNECTED");
                printWriter.println(cmd);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
