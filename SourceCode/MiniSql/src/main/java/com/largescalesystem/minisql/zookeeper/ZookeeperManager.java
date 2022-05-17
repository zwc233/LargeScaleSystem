package com.largescalesystem.minisql.zookeeper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class ZookeeperManager implements Runnable{

    public static final String ZK_HOST = "localhost:2181";
    //ZooKeeper会话超时时间
    public static final Integer ZK_SESSION_TIMEOUT = 3000;
    //ZooKeeper连接超时时间
    public static final Integer ZK_CONNECTION_TIMEOUT = 3000;
    //ZooKeeper集群内各个服务器注册的节点路径
    public static final String ZNODE = "/db";
    //ZooKeeper集群内各个服务器注册自身信息的节点名前缀
    public static final String HOST_NAME_PREFIX = "Region_";

    @Override
    public void run() {
        this.startMonitor();
    }

    public void startMonitor(){
        try {
           CuratorFramework client = ZooKeeperUtils.createAndStartClient(ZNODE);
           if(!ZooKeeperUtils.checkNode(client,ZNODE)){
               ZooKeeperUtils.createNode(client,ZNODE);
           }
        } catch (Exception e) {
            log.warn(e.getMessage(),e);
        }
    }
}
