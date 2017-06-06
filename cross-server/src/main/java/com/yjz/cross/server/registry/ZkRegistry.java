package com.yjz.cross.server.registry;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.yjz.cross.CrossException;
import com.yjz.cross.server.init.CrossServerInitializer;

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
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    private static final String ROOT_NODE_PATH = "/cross";
    
    private String registryName = null;
    
    private volatile ZooKeeper zk = null;
    
    ReentrantLock lock = new ReentrantLock();
    
    private CountDownLatch latch = new CountDownLatch(1);
    
    private String zkAddress;
    
    private String serverAddress;
    
    protected ZkRegistry(String registryName)
    {
        this.registryName = registryName;
        this.zkAddress = CrossServerInitializer.CONFIGURATION.getRegistryAddress(registryName);
        this.serverAddress = CrossServerInitializer.CONFIGURATION.getServerAddress();
    }
    
    public void regist(String serviceName)
    {
        if (zk == null)
        {
            try
            {
                lock.lock();
                
                if (zk == null)
                {
                    connectZk();
                }
            }
            finally
            {
                lock.unlock();
                
            }
        }
        
        createRootNode();
        createServiceNode(serviceName);
        createSerivceAddressNode(serviceName);
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
            throw new CrossException("'zk.address' is not configured in 'application.properties'!");
        }
        
        // 已连接则直接返回
        if(isZkConnected())
        {
            return;
        }
        
        try
        {
            logger.info("Cross server connecting to zkServer " + zkAddress);
            
            zk = new ZooKeeper(zkAddress, 8000, new Watcher()
            {
                
                @Override
                public void process(WatchedEvent event)
                {
                    if (event.getState() == Event.KeeperState.SyncConnected)
                    {
                        logger.info("Cross server connected to zkServer " + zkAddress);
                        latch.countDown();
                    }
                }
            });
            
            latch.await();
        }
        catch (IOException | InterruptedException e)
        {
            logger.error(e.getMessage(), e);
            throw new CrossException(e);
        }
    }
    
    private boolean isZkConnected()
    {
        if (zk == null)
        {
            try
            {
                lock.lock();
                if (zk != null)
                {
                    return true;
                }
                return false;
            }
            finally
            {
                lock.unlock();  
            }
        }
        
        return true;
    }
    
    private void createRootNode()
    {
        if (zk != null)
        {
            try
            {
                Stat state = zk.exists(ROOT_NODE_PATH, false);
                if (state == null)
                {
                    zk.create(ROOT_NODE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
            catch (KeeperException | InterruptedException e)
            {
                logger.error(e.getMessage(), e);
                throw new CrossException(e);
            }
        }
    }
    
    private void createServiceNode(String serviceName)
    {
        if (zk != null)
        {
            try
            {
                String servicePath = ROOT_NODE_PATH + "/" + serviceName;
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
    
    private void createSerivceAddressNode(String serviceName)
    {
        if (zk != null)
        {
            try
            {
                String localAddress = getLocalAddress();
                String serviceAddressPath = ROOT_NODE_PATH + "/" + serviceName + "/" + localAddress;
                
                Stat state = zk.exists(serviceAddressPath, false);
                if (state == null)
                {
                    zk.create(serviceAddressPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
            catch (KeeperException | InterruptedException e)
            {
                logger.error(e.getMessage(), e);
                throw new CrossException(e);
            }
        }
    }
    
    private String getLocalAddress()
    {
        if (this.serverAddress != null)
        {
            return this.serverAddress;
        }
        
        try
        {
            Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            
            InetAddress ip = null;
            while (allNetInterfaces.hasMoreElements())
            {
                NetworkInterface netInterface = (NetworkInterface)allNetInterfaces.nextElement();
                
                Enumeration addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements())
                {
                    ip = (InetAddress)addresses.nextElement();
                    if (ip != null && ip instanceof Inet4Address)
                    {
                        return ip.getHostAddress();
                    }
                }
            }
            
            logger.error("can't get local address!");
            throw new CrossException("can't get local address!");
        }
        catch (SocketException e)
        {
            logger.error(e.getMessage(), e);
            throw new CrossException(e);
        }
    }
    
}
