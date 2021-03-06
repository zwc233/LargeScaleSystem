package com.largescalesystem.minisql.zookeeper;

import java.nio.charset.StandardCharsets;
import java.util.List;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperUtils {
    /**
     * 创建客户端，默认连接本地2181端口
     * @return CuratorFramework
	 */
    public static CuratorFramework createAndStartClient() {
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 3000, 3000, exponentialBackoffRetry);
        //启动客户端
        client.start();

        return client;
    }

    /**
     * 创建客户端，默认远程2181端口
     * @param ip 远程zookeeper服务器ip
     * @return CuratorFramework
	 */
    public static CuratorFramework createAndStartClient(String ip) {
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient(ip + ":2181", 3000, 3000, exponentialBackoffRetry);
        //启动客户端
        client.start();

        return client;
    }

	/**
     * 关闭客户端
	 */
	public static void closeClient(CuratorFramework client) {
       client.close();
    }
    /**
	 *
	 * 获取子节点列表 打印
     * @param client 传入客户端
	 * @param parentPath 节点路径
	 * @return List<String>
	 */
	public static List<String> nodesList(CuratorFramework client, String parentPath) throws Exception {
		Stat stat = client.checkExists().forPath(parentPath);
		if (stat == null) {
			return null;
		}
		return client.getChildren().forPath(parentPath);
	}

    // TODO 一个更加完善的遍历(比如层序遍历所有节点获取table信息的函数等等)

    /**
	 * 递归创建一个节点，没有默认值
     * @param client 传入客户端
	 * @param path 节点路径
	 * @return void
	 */
    public static Boolean createNode(CuratorFramework client, String path) throws Exception{
        Stat stat = client.checkExists().forPath(path);
		if (stat == null) {
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
		} else {
            System.out.println("节点目录已存在");
            return false; 
        }
        return true;
    }

    /**
	 * 递归创建一个节点，有默认值
     * @param client 传入客户端
	 * @param path 节点路径
	 * @return void
	 */
    public static Boolean createNode(CuratorFramework client, String path, String value) throws Exception{
        Stat stat = client.checkExists().forPath(path);
		if (stat == null) {
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, value.getBytes());
		} else {
            System.out.println("节点目录已存在"); 
            return false;
        }
        return true;
    }

    /**
	 * 获取一个节点的信息, 返回一个字符串
     * @param client 传入客户端
	 * @param path 节点路径
	 * @return String
	 */
    public static String getDataNode(CuratorFramework client, String path) throws Exception {
		Stat stat = client.checkExists().forPath(path);
		if (stat == null) {
			return null;
		}
		byte[] data = client.getData().forPath(path);
		return new String(data, StandardCharsets.UTF_8);
	}

    /**
	 *
	 * 设置节点中的信息
	 * @param client 传入客户端
	 * @param path 节点路径
	 * @param message 传入信息
	 * @return boolean
	 */
	public static Boolean setDataNode(CuratorFramework client, String path, String message) throws Exception {
		Stat stat = client.checkExists().forPath(path);
		if (stat == null) {
			return false;
		}
		client.setData().withVersion(stat.getVersion()).forPath(path, message.getBytes());
		return true;
	}

	/**
	 * 删除该节点以及该节点下子节点
	 * @param client 传入客户端
	 * @param path 节点路径
     * @return boolean
	 */
	public static Boolean delNodeAndChild(CuratorFramework client, String path) throws Exception {
		Stat stat = client.checkExists().forPath(path);
		if (stat == null) {
			return false;
		}
		client.delete().deletingChildrenIfNeeded().forPath(path);
		return true;
	}

	/**
	 * 删除当前节点
	 * @param client 传入客户端
	 * @param path 节点路径
	 * @return boolean
	 */
	public static Boolean delCurrentNode(CuratorFramework client, String path) throws Exception {
		Stat stat = client.checkExists().forPath(path);
		if (stat == null) {
			return false;
		}
		client.delete().forPath(path);
		return true;
	}

	/**
	 * 检查节点是否存在
	 * @param client 传入客户端
	 * @param path 节点路径
	 * @return boolean
	 */
	public static Boolean checkNode(CuratorFramework client, String path) throws Exception {
		Stat stat = client.checkExists().forPath(path);
		return stat != null;
	}
	
	/**
         * 若不存在节点则创建
         * @param client
         * @param path
         * @throws Exception
         */

        public void createIfNeed(CuratorFramework client, String path) throws Exception {
            if(!checkNode(client, path)){
                Boolean bool = createNode(client, path);
            }

        }
 
        /**
         * 监听某个节点
         * @param client
         * @param path
         */
    	public void nodeCuratorCache(CuratorFramework client, String path) {
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
