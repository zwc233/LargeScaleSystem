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

import java.net.Inet4Address;
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
        String brokenRegion = null;
        List<String> regionsNew = client.getChildren().forPath("/lss/region_servers");
        try{
            for(String region : regions){
                if(!regionsNew.contains(region)){
                    brokenRegion = region;
                    int index = regions.indexOf(brokenRegion);
                    String[] nodeInfo = nodeList.get(index).split(",");
                    nodeList.remove(index);
                    ArrayList<String> tableList = new ArrayList<>(Arrays.asList(nodeInfo).subList(6, nodeInfo.length));
                    for(String table : tableList){
                        if(Objects.equals(table.split("_")[table.split("_").length - 1], "slave")){
                            StringBuilder tableName = new StringBuilder();
                            for(int i = 0; i < table.split("_").length - 1; i++){
                                tableName.append(table.split("_")[i]);
                            }
                            int indexList = updateSlave(table);
                            if(indexList != -1){
                                String cmd = "dump " + tableName + " @ " + regions.get(indexList) + "," + nodeInfo[0] + "," + nodeInfo[1]
                                        + ",root,123";
                                ServerMaster.dumpTable(cmd);
                            }
                        }
                        else {
                            int min = 100;
                            int indexTmp = 0;
                            for(String node : nodeList){
                                if(node.split(",").length < min){
                                    min = node.split(",").length;
                                    indexTmp = nodeList.indexOf(node);
                                }
                            }
                            String cmd = "migrate " + table + " @ " + regions.get(indexTmp) + "," + nodeList.get(indexTmp).split(",")[0]
                                    + "," + nodeList.get(indexTmp).split(",")[1] + "root,123";
                            ServerMaster.migrateTable(cmd);
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
        for(int i = 0; i < op.length - 1; i++){
            tableName.append(op[i]);
            if(i < op.length - 2){
                tableName.append("_");
            }
        }
        int min = 100;
        int index = -1;
        for(String node : nodeList){
            if(!(Arrays.asList(node.split(",")).contains(tableName.toString())
                    || Arrays.asList(node.split(",")).contains(table))){
                if(node.split(",").length < min){
                    min = node.split(",").length;
                    index = nodeList.indexOf(node);
                }
            }
        }
        return index;
    }
}
