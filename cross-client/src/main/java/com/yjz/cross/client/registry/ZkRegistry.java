package com.yjz.cross.client.registry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.CrossException;
import com.yjz.cross.client.init.CrossClientInitializer;
import com.yjz.cross.client.transport.ClientHandler;
import com.yjz.cross.client.transport.ClientHandlerManager;
import com.yjz.cross.client.transport.ConnectionManager;
import com.yjz.cross.client.util.CommonUtil;

/**
 * 
 * @ClassName ZkRegistry
 * @Description
 * @author biw
 * @Date 2017.5.16 02:49:54
 * @version 1.0.0
 */
public class ZkRegistry implements Registry
{
    private static final Logger logger = LoggerFactory.getLogger(ZkRegistry.class);
    
    private static final String SERVER_ROOT_NODE_PATH = "/cross-server";
    
    private static final String CLIENT_ROOT_NODE_PATH = "/cross-client";
    
    private static final String LOCK_ROOT_NODE_PATH = "/locks-server";
    
    private String registryName = null;
    
    private ZooKeeper zk = null;
    
    private CountDownLatch latch = new CountDownLatch(1);
    
    private String zkAddress;
    
    private Set<String> serviceClassNameSet = new HashSet<>();
    
    private ZkNodeMutexManager mutexManager = null;
    
    protected ZkRegistry(String registryName)
    {
        this.registryName = registryName;
        this.zkAddress = CrossClientInitializer.CONFIGURATION.getRegistryAddress(registryName);
        this.mutexManager = new ZkNodeMutexManager(this.zkAddress);
        
    }
    
    public void setServiceClassName(Set<String> serviceClassNameList)
    {
        serviceClassNameSet.addAll(serviceClassNameList);
    }
    
    public void setServiceClass(Set<Class<?>> serviceClassList)
    {
        for (Class<?> serviceClass : serviceClassList)
        {
            serviceClassNameSet.add(serviceClass.getName());
        }
    }
    
    public List<String> getServiceAddresses(String serviceClassName)
    {
        if (zk == null)
        {
            connectZk();
        }
        
        if (zk != null)
        {
            try
            {
                String servicePath = SERVER_ROOT_NODE_PATH + "/" + serviceClassName;
                List<String> addrList = zk.getChildren(servicePath, false);
                return addrList;
            }
            catch (KeeperException | InterruptedException e)
            {
                logger.error(e.getMessage());
                return new ArrayList<String>();
            }
        }
        
        return new ArrayList<String>();
    }
    
    /**
     * @Description 连接到Zookeeper
     * @author biw
     */
    private void connectZk()
    {
        if (zkAddress == null)
        {
            logger.error("'zk.address' is not configured in 'application.properties'!");
        }
        
        synchronized (SERVER_ROOT_NODE_PATH)
        {
            if (zk == null)
            {
                try
                {
                    logger.info("Cross client connecting to zk server " + zkAddress);
                    
                    zk = new ZooKeeper(zkAddress, 60000, new Watcher()
                    {
                        @Override
                        public void process(WatchedEvent event)
                        {
                            if (event.getState() == Event.KeeperState.SyncConnected)
                            {
                                logger.info("Cross client connected to zk server " + zkAddress);
                                latch.countDown();
                            }
                        }
                    });
                    
                    latch.await();
                    
                    /** 设置Watcher，在检测到zookeeper会话失效或者连接中断时设置zk为null，并重连 */
                    zk.register(new Watcher()
                    {
                        @Override
                        public void process(WatchedEvent event)
                        {
                            if (event.getState() == Event.KeeperState.Expired
                                || event.getState() == Event.KeeperState.Disconnected)
                            {
                                logger.info("Cross Client disconnected from zkServer " + zkAddress);
                                zk = null;
                                watchServerRootAndServices();
                            }
                        }
                    });
                }
                catch (IOException | InterruptedException e)
                {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
    
    @Override
    public void watchServerRootAndServices()
    {
        watchRoot(false);
        
        for (String serviceClassName : serviceClassNameSet)
        {
            watchService(serviceClassName, false);
        }
    }
    
    /**
     * @Description 检查服务是否发生变更，变更则更新对应ClientHandlerManager
     * @author biw
     * @param serviceClassName
     */
    private void watchService(String serviceClassName, boolean updateFlag)
    {
        if (zk == null)
        {
            connectZk();
        }
        
        if (zk == null)
        {
            return;
        }
        
        try
        {
            String servicePath = SERVER_ROOT_NODE_PATH + "/" + serviceClassName;
            List<String> serverAddrList = zk.getChildren(servicePath, new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    if (event.getType() == Event.EventType.NodeChildrenChanged)
                    {
                        watchService(serviceClassName, true);
                    }
                }
            });
            
            if (updateFlag && !serverAddrList.isEmpty())
            {
                logger.info(serviceClassName + " adrresses: {}", serverAddrList);
                logger.info("Service discovery triggered updating connected server node for :" + serviceClassName);
                updateClientHanderManager(serviceClassName, serverAddrList);
            }
        }
        catch (KeeperException | InterruptedException e)
        {
            logger.error(e.getMessage());
        }
    }
    
    /**
     * 更新服务对应的ClientHandlers
     * 
     * @Description (TODO这里用一句话描述这个方法的作用)
     * @author biw
     * @param serviceClassName
     * @param serverAddrList
     */
    private void updateClientHanderManager(String serviceClassName, List<String> serverAddrList)
    {
        ClientHandlerManager handlerManager = ConnectionManager.instance().getHandlerManager(serviceClassName);
        if (handlerManager != null)
        {
            handlerManager.updateClientHandler(serverAddrList);
        }
    }
    
    /**
     * @Description 检查根节点是否发生变更，变更则更新对应ClientHandlerManager
     * @author biw
     */
    private void watchRoot(boolean updateFlag)
    {
        if (zk == null)
        {
            connectZk();
        }
        
        if (zk == null)
        {
            return;
        }
        
        try
        {
            List<String> serviceClassNameList = zk.getChildren(SERVER_ROOT_NODE_PATH, new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    if (event.getType() == Event.EventType.NodeChildrenChanged)
                    {
                        watchRoot(true);
                    }
                }
            });
            
            if (updateFlag)
            {
                logger.info("Root Service discovery triggered updating connected service node.");
                serviceClassNameList = (serviceClassNameList == null) ? new ArrayList<String>(0) : serviceClassNameList;
                updateService(serviceClassNameList);
            }
        }
        catch (KeeperException | InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
        
    }
    
    /**
     * 更新所有新增或者删除Service的ClientHandler
     * 
     * @Description
     * @author biw
     * @param serviceList
     */
    private void updateService(List<String> serviceClassNameList)
    {
        Map<String, ClientHandlerManager> handlerManagers = ConnectionManager.instance().getHandlerManagers();
        
        // 处理新增的service
        Set<String> serviceClassNameSet = new HashSet<>();
        for (String serviceClassName : serviceClassNameList)
        {
            if (!handlerManagers.containsKey(serviceClassName))
            {
                serviceClassNameSet.add(serviceClassName);
            }
        }
        ConnectionManager.instance().concurrentConnectServerByClassName(serviceClassNameSet);
        
        // 处理删除的service
        for (Entry<String, ClientHandlerManager> entry : handlerManagers.entrySet())
        {
            boolean matchFlag = false;
            for (String serviceClassName : serviceClassNameList)
            {
                if (entry.getKey().equals(serviceClassName))
                {
                    matchFlag = true;
                    break;
                }
            }
            
            if (!matchFlag)
            {
                ClientHandlerManager handlerManager = ConnectionManager.instance().getHandlerManager(entry.getKey());
                handlerManager.clearClientHandlers();
            }
        }
    }
    
    public void stop()
    {
        if (zk != null)
        {
            try
            {
                zk.close();
            }
            catch (InterruptedException e)
            {
                logger.error(e.getMessage(), e);
                throw new CrossException(e);
            }
        }
    }
    
    @Override
    public void registClientForServer(String serviceClassName, String serviceAddress, String localAddress)
    {
        if (zk == null)
        {
            connectZk();
        }
        
        if (zk != null)
        {
            InterProcessMutex lock = null;
            try
            {
                String serverAddressPath = SERVER_ROOT_NODE_PATH + "/" + serviceClassName + "/" + serviceAddress;
                Stat state = zk.exists(serverAddressPath, false);
                if (state != null)
                {
                    lock = this.mutexManager
                        .getInterProcessMutex(LOCK_ROOT_NODE_PATH + "/" + serviceClassName + "/" + serviceAddress);
                    lock.acquire();
                    
                    byte[] data = zk.getData(serverAddressPath, false, state);
                    String oldClientAddr = new String(data);
                    String newClientAddr = oldClientAddr + ";" + localAddress;
                    zk.setData(serverAddressPath, newClientAddr.getBytes(), -1);
                }
            }
            catch (Exception e)
            {
                logger.error(e.getMessage(), e);
            }
            finally
            {
                if (lock != null)
                {
                    try
                    {
                        lock.release();
                    }
                    catch (Exception e)
                    {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }
    }
    
    public void registClientRoot()
    {
        this.createClientRootNode();
        
        Map<String, ClientHandlerManager> handlerManagerMap = ConnectionManager.instance().getHandlerManagers();
        for (ClientHandlerManager chm : handlerManagerMap.values())
        {
            String serviceClassName = chm.getServiceClassName();
            this.createClientServiceNode(serviceClassName);
            
            List<ClientHandler> clientHandlerList = chm.getClientHandlerList();
            for (ClientHandler ch : clientHandlerList)
            {
                String serviceAddress = CommonUtil.getServiceAddress(ch.getRemotePeer());
                String localAddress = CommonUtil.getServiceAddress(ch.getLocalPeer());
                this.createClientAddrNode(serviceClassName, serviceAddress, localAddress);
            }
        }
    }
    
    public void registClientService(String serviceClassName)
    {
        if (zk == null)
        {
            return;
        }
        
        createClientServiceNode(serviceClassName);
    }
    
    private void createClientRootNode()
    {
        if (zk == null)
        {
            connectZk();
        }
        
        if (zk != null)
        {
            try
            {
                Stat state = zk.exists(CLIENT_ROOT_NODE_PATH, false);
                if (state == null)
                {
                    zk.create(CLIENT_ROOT_NODE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
            catch (KeeperException | InterruptedException e)
            {
                logger.error(e.getMessage(), e);
                throw new CrossException(e);
            }
        }
    }
    
    private void createClientServiceNode(String serviceClassName)
    {
        if (zk != null)
        {
            try
            {
                String servicePath = CLIENT_ROOT_NODE_PATH + "/" + serviceClassName;
                Stat state = zk.exists(servicePath, false);
                if (state == null)
                {
                    zk.create(servicePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
            catch (KeeperException | InterruptedException e)
            {
                logger.error(e.getMessage(), e);
                throw new CrossException(e);
            }
        }
    }
    
    public void createClientAddrNode(String serviceClassName, String serviceAddress, String localAddress)
    {
        if (zk != null)
        {
            try
            {
                String servicePath = CLIENT_ROOT_NODE_PATH + "/" + serviceClassName;
                Stat state = zk.exists(servicePath, false);
                if (state != null)
                {
                    zk.setData(servicePath, serviceAddress.getBytes(), -1);
                }
            }
            catch (KeeperException | InterruptedException e)
            {
                logger.error(e.getMessage(), e);
                throw new CrossException(e);
            }
        }
        
    }
    
    private static class ZkNodeMutexManager
    {
        private CuratorFramework zkclient = null;
        
        private Map<String, InterProcessMutex> zkNodeMutexMap = new ConcurrentHashMap<>();
        
        public ZkNodeMutexManager(String zkAddress)
        {
            this.zkclient = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
            this.zkclient.start();
        }
        
        public InterProcessMutex getInterProcessMutex(String lockPath)
        {
            if (zkNodeMutexMap.containsKey(lockPath))
            {
                return zkNodeMutexMap.get(lockPath);
            }
            
            InterProcessMutex lock = new InterProcessMutex(this.zkclient, lockPath);
            zkNodeMutexMap.put(lockPath, lock);
            
            return lock;
        }
    }
}
