package com.largescalesystem.minisql.master;

import com.largescalesystem.minisql.zookeeper.ZooKeeperUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.largescalesystem.minisql.master.RegionManager.listenNode;

public class Server {
    public static CuratorFramework client = null;
    public static String address = "192.168.188.136:2181";
    public static ArrayList<String> nodeList = new ArrayList<>();
    public static String masterInfo = null;
    public static List<String> regions = null;

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder().connectString(address).sessionTimeoutMs(3000)
                .connectionTimeoutMs(3000).retryPolicy(retryPolicy).build();
        client.start();
        ServerSocket server = new ServerSocket(2000);
        System.out.println("Master is waiting for connection");
        for(;;){
//            handleMistake mistake = new handleMistake();
//            mistake.start();
            Socket client = server.accept();
            ClientHandler clientHandler = new ClientHandler(client);
            clientHandler.start();
        }
    }

    private static class ClientHandler extends Thread{
        private final Socket socket;

        ClientHandler(Socket socket){
            this.socket = socket;
        }

        @Override
        public void run(){
            super.run();
            System.out.println("New client connected, address: " + socket.getInetAddress() + ", port: " + socket.getPort());
            try {
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String temp = input.readLine();
                String sql = input.readLine();
                System.out.println("CLIENT REQUEST: " + sql);
                System.out.println("NodeList: " + nodeList);
                ArrayList<String> tableInfo = getNodeList();
                if(tableInfo.size() != nodeList.size()){
                    listenNode();
                    tableInfo = getNodeList();
                }
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                String tableName = null;
                if (Objects.equals(sql.split(" ")[0], "insert") || Objects.equals(sql.split(" ")[0], "delete") ||
                        Objects.equals(sql.split(" ")[0], "drop")) {
                    tableName = sql.split(" ")[2];
                }
                else if(Objects.equals(sql.split(" ")[0], "select")){
                    tableName = sql.split(" ")[3];
                }
                else if(Objects.equals(sql.split(" ")[0], "create")){
                    tableName = sql.split("\\(")[0].split(" ")[2];
                }
                System.out.println(tableName);
                if(Objects.equals(sql.split(" ")[0], "create")){
                    System.out.println("TARGET REGION SERVER: " + loadBalance(tableInfo));
                    printWriter.println(loadBalance(tableInfo));
                }
                else if (Objects.equals(sql.split(" ")[0], "select")){
                    StringBuilder result = new StringBuilder();
                    for (String nodeInfo : tableInfo) {
                        String[] list = nodeInfo.split(",");
                        boolean flag = false;
                        for (String s : list) {
                            if (Objects.equals(s, tableName) || Objects.equals(s.split("_")[0], tableName)) {
                                flag = true;
                                break;
                            }
                        }
                        if (flag) {
                            System.out.println("TARGET REGION SERVER: " + nodeInfo);
                            if(result.toString().equals("")){
                                result.append(nodeInfo).append(";");
                            }
                            else{
                                result.append(nodeInfo);
                            }
                            break;
                        }
                    }
                    printWriter.println(result);
                }
                else {
                    StringBuilder result = new StringBuilder();
                    for (String nodeInfo : tableInfo) {
                        String[] list = nodeInfo.split(",");
                        boolean flag = false;
                        for (String s : list) {
                            if (Objects.equals(s, tableName) || Objects.equals(s.split("_")[0], tableName)) {
                                flag = true;
                                break;
                            }
                        }
                        if (flag) {
                            System.out.println("TARGET REGION SERVER: " + nodeInfo);
                            if(result.toString().equals("")){
                                result.append(nodeInfo).append(";");
                            }
                            else{
                                result.append(nodeInfo);
                            }
                        }
                    }
                    printWriter.println(result);
                }
                printWriter.close();

//                listenNode();
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

    public static ArrayList<String> getNodeList() throws Exception {
        ArrayList<String> tmpNodeList;
        List<String> tmpRegion;
        tmpRegion = getRegion();
        tmpNodeList = getNodeList(tmpRegion);
        if((tmpNodeList.size() == nodeList.size() && !(tmpNodeList.containsAll(nodeList) && nodeList.containsAll(tmpNodeList)))
                || nodeList.size() == 0){
            nodeList = tmpNodeList;
            System.out.println("New List: " + nodeList);
            regions = tmpRegion;
        }
//        getRegionAndMaster();
        nodeCuratorCache(client, "/lss/region_servers");
        return tmpNodeList;
    }

    public static List<String> getRegion() throws Exception {
        return client.getChildren().forPath("/lss/region_servers");
    }

    public static ArrayList<String> getNodeList(List<String> regions) throws Exception {
        ArrayList<String> tmp = new ArrayList<>();
        for(String region : regions){
            System.out.println(ZooKeeperUtils.getDataNode(client, "/lss/region_servers/" + region));
            tmp.add(ZooKeeperUtils.getDataNode(client, "/lss/region_servers/" + region));
        }
        return tmp;
    }

    public static void getRegionAndMaster() {
        try{
            regions = client.getChildren().forPath("/lss/region_servers");
            System.out.println("Region: " + regions);
            for(String region : regions){
                System.out.println(ZooKeeperUtils.getDataNode(client, "/lss/region_servers/" + region));
                nodeList.add(ZooKeeperUtils.getDataNode(client, "/lss/region_servers/" + region));
            }
            System.out.println("NodeList: " + nodeList);
//            masterInfo = ZooKeeperUtils.getDataNode(client, "/lss/master");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    public static void listenNode() throws Exception {
        String brokenRegion;
        // 重新获取当前活跃节点
        List<String> regionsNew = client.getChildren().forPath("/lss/region_servers");
        System.out.println("New regions: " + regionsNew);
        System.out.println("NodeList: " + nodeList);
        System.out.println("Old Regions: " + regions);
        try{
            for(String region : regions){
                // 发现某一节点宕机
                if(!regionsNew.contains(region)){
                    System.out.println("Broken node: " + region);
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
                                // 发送socket请求
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
                                String mainServer = null;
                                for(String tmp : nodeList){
                                    if(nodeList.indexOf(tmp) != indexMain){
                                        mainServer = tmp;
                                        break;
                                    }
                                }
                                nodeInfo = nodeList.get(indexMain).split(",");
                                String[] mainList = mainServer.split(",");
                                String cmd = "dump " + table + " @ " + regions.get(nodeList.indexOf(mainServer)) + "," + mainList[0] + "," + mainList[1]
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
}
