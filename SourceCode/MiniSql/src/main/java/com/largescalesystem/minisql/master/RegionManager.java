package com.largescalesystem.minisql.master;

import com.largescalesystem.minisql.zookeeper.ZooKeeperUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

public class RegionManager{
    public static CuratorFramework client = null;
    public String address = "127.0.0.1:2181";
    public static List<String> nodeList = null;
    public static String masterInfo = null;

    public void main(String[] args) throws Exception{
        try{
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            client = CuratorFrameworkFactory.builder().connectString(address).sessionTimeoutMs(3000)
                    .connectionTimeoutMs(3000).retryPolicy(retryPolicy).build();
            client.start();
            getRegionAndMaster();
            nodeCuratorCache(client, "/lss/region_servers");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getRegionAndMaster() {
        try{
            List<String> regions = client.getChildren().forPath("/lss/region_servers");
            for(String region : regions){
                nodeList.add(ZooKeeperUtils.getDataNode(client, "/lss/region/servers/" + region));
            }
            masterInfo = ZooKeeperUtils.getDataNode(client, "/lss/master");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String> getNodeList() {
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
                .forCreates(node -> System.out.println(String.format("Node created: [%s]", node)))
                .forChanges((oldNode, node) -> System.out.println(String.format("Node changed. Old: [%s] New: [%s]", oldNode, node)))
                .forDeletes(oldNode -> System.out.println(String.format("Node deleted. Old value: [%s]", oldNode)))
                .forInitialized(() -> System.out.println("Cache initialized"))
                .build();

        cache.listenable().addListener(listener);

        cache.start();
    }

}
