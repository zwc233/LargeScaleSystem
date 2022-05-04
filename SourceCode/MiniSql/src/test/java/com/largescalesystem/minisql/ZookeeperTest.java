package com.largescalesystem.minisql;

import static org.junit.Assert.assertTrue;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.util.List;

/**
 * Unit test for simple App.
 */
public class ZookeeperTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }
    @Test
    public void myTest() throws Exception {
        System.out.println("测试开始");
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 3000, 3000, exponentialBackoffRetry);
        client.start();
        try {
            client.create().forPath("/test", "hello world".getBytes());
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("该节点已经存在啦");
        }

        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/server1");
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("该节点已经存在啦");
        }
        Thread.sleep(5000);
        client.close();
    }
    @Test
    public void method() throws Exception {
        //重试策略
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 3000, 3000, exponentialBackoffRetry);
        //启动客户端
        client.start();

        client.create().forPath("/testapi");
        //创建一个有内容的节点数据
        client.create().forPath("/a","hello world".getBytes());
        //创建一个多级目录的节点
        client.create().creatingParentsIfNeeded().forPath("/b/b1/b2","good".getBytes());
        //创建一个带序号的持久节点 PERSISTENT_SEQUENTIAL:带序号的 相当于命令行的 -s
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/c/c1","niubi".getBytes());
        //创建临时节点 设置时延5秒关闭
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/d","nb".getBytes());
        //创建临时有序节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/e","6666".getBytes());
        //设置时延5秒关闭
        Thread.sleep(5000);
        client.close();
    }
    @Test
    public void method2() throws Exception {
        //重试策略
        RetryPolicy exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 3000, 3000, exponentialBackoffRetry);
        //启动客户端
        client.start();

        //修改节点数据
        client.setData().forPath("/testapi","setData".getBytes());

        //关闭客户端
        client.close();
    }
    @Test
    public void method3() throws Exception {
        //重试策略
        RetryPolicy exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 3000, 3000, exponentialBackoffRetry);
        //启动客户端
        client.start();

        //删除单节点
        client.delete().forPath("/testapi");
        //删除多层级结构的节点
        client.delete().deletingChildrenIfNeeded().forPath("/b");
        //强制删除节点，只要客户端会话有效，那么会持续删除，直到节点删除成功
        //例如遇到一些网络异常的情况的时候
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/c");
        //
        client.close();
    }
    @Test
    public void method4() throws Exception {
        //重试策略
        RetryPolicy exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 3000, 3000, exponentialBackoffRetry);
        //启动客户端
        client.start();

        //创建节点
        // client.create().creatingParentsIfNeeded().forPath("/a","good".getBytes());
        //查询节点数据
        byte[] bytes = client.getData().forPath("/a");
        System.out.println(new String(bytes));

        client.close();
    }
    @Test
    public void method5() throws Exception {
        //重试策略
        RetryPolicy exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3000);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 1000, 1000, exponentialBackoffRetry);
        //启动客户端
        client.start();

        //Watch监听机制(NodeCache,PathChildrenCache,TreeCache)
        //NodeCache监听本节点
        //PathChildrenCache监听子节点
        //TreeCache监听本节点加子节点
        final NodeCache nodeCache = new NodeCache(client, "/a");
        //true代表初始化时就获取节点的数据并且缓存到本地
        nodeCache.start(true);

        //获取节点数据,初始化
        System.out.println(new String(nodeCache.getCurrentData().getData()));

        //添加监听回调事件
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                //值修改了就打印一下
                System.out.println(new String(nodeCache.getCurrentData().getData()));
            }
        });

        //此处不能关闭客户端
        //阻塞进程
        System.in.read();
    }
    @Test
    public void method6() throws Exception {
        //重试策略
        RetryPolicy exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,3000);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 1000, 1000, exponentialBackoffRetry);
        //启动客户端
        client.start();
        client.create().creatingParentsIfNeeded().forPath("/q/q1/q2", "test".getBytes());
        //true代表把节点内容缓存起来
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/q/q1/q2",true);
        /*
         * POST_INITIALIZED_EVENT表示在启动时缓存子节点数据，并且提示初始化
         * NORMAL普通启动方式，在启动时缓存子节点数据
         * BUILD_INITIAL_CACHE：在启动时什么都不会输出，这种模式在执行start之前会先执行rebuild方法，而该方法不会发出任何通知
         * */
        /*
         * StartMode：初始化方式
         * POST_INITIALIZED_EVENT：异步初始化。初始化后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         * */
        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        List<ChildData> childDataList = pathChildrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点数据列表:");
        for(ChildData cd : childDataList){
            String childData = new String(cd.getData());
            System.out.println(childData);
        }
        //添加监听
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework ient, PathChildrenCacheEvent event) throws Exception {
                if(event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)){
                    System.out.println("子节点初始化成功");
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
                    System.out.println("添加子节点路径:"+event.getData().getPath());
                    System.out.println("子节点数据:"+new String(event.getData().getData()));
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)){
                    System.out.println("删除子节点:"+event.getData().getPath());
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){
                    System.out.println("修改子节点路径:"+event.getData().getPath());
                    System.out.println("修改子节点数据:"+new String(event.getData().getData()));
                }
            }
        });
        //此处不能关闭客户端
        //阻塞进程
        System.in.read();
    }
}
