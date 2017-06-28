package com.yjz.cross.client.transport;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.CrossException;
import com.yjz.cross.client.registry.Registry;
import com.yjz.cross.client.registry.RegistryFactory;
import com.yjz.cross.client.util.CommonUtil;
import com.yjz.cross.codec.RpcDecoder;
import com.yjz.cross.codec.RpcEncoder;
import com.yjz.cross.codec.RpcResponse;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class ConnectionManager
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    
    private static ConnectionManager connManager = new ConnectionManager();
    
    // 负责连接服务端的线程池
    private ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(0, Integer.MAX_VALUE, 600L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    
    // 每个服务类对应一个clientHandlerManager
    private Map<String, ClientHandlerManager> clientHandlerManagerMap = new ConcurrentHashMap<>();
    
    private Map<String, ReentrantLock> connectServerLockMap = new HashMap<>();
    
    private ConnectionManager()
    {
    }
    
    public static ConnectionManager instance()
    {
        return connManager;
    }
    
    public Map<String, ClientHandlerManager> getHandlerManagers()
    {
        return this.clientHandlerManagerMap;
    }
    
    /**
     * @Description 根据服务全名获取对应客户端处理程序管理器
     * @author biw
     * @param serviceName
     * @return
     */
    public ClientHandlerManager getHandlerManager(String serviceClassName)
    {
        ClientHandlerManager handlerManager = null;
        
        if (!clientHandlerManagerMap.containsKey(serviceClassName))
        {
            handlerManager = new ClientHandlerManager(serviceClassName);
            clientHandlerManagerMap.put(serviceClassName, handlerManager);
        }
        else
        {
            handlerManager = clientHandlerManagerMap.get(serviceClassName);
        }
        
        return handlerManager;
    }
    
    /**
     * 针对每一个服务代理类，并发的与已经注册的服务端建立连接
     * @Description 监控根节点发生变更时触发
     * @author biw
     * @param serviceClassName
     * @param serviceAddresses
     * @param latch 控制每个连接建立完毕才进行下一步处理
     */
    public void concurrentConnectServerByClassName(Set<String> serviceClassNameSet)
    {
        Registry registry = RegistryFactory.instance().getRegistry();
        registry.setServiceClassName(serviceClassNameSet);
        
        for (String serviceClassName : serviceClassNameSet)
        {
            List<String> serviceAddresses = registry.getServiceAddresses(serviceClassName);
            
            if (!serviceAddresses.isEmpty())
            {
                threadPoolExecutor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        syncConnectServer(serviceClassName, serviceAddresses);
                    }
                });
            }
        }
    }
    
    /**
     * 针对每一个服务，与已经注册该服务的服务端的建立连接
     * 
     * @Description 监控单个服务下新增的结点
     * @author biw
     * @param serviceClassName
     * @param serviceAddresses
     */
    public void connectServer(String serviceClassName, List<InetSocketAddress> socketAddresses)
    {
        List<String> serviceAdressList = new ArrayList<>();
        for (InetSocketAddress socketAddr : socketAddresses)
        {
            serviceAdressList.add(socketAddr.getHostName() + ":" + socketAddr.getPort());
        }
        
        threadPoolExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                syncConnectServer(serviceClassName, serviceAdressList);
            }
        });
//        syncConnectServer(serviceClassName, serviceAdressList);
        
    }
    
    /**
     * 针对每一个服务代理类，并发的与已经注册的服务端建立连接
     * 
     * @Description 启动时调用
     * @author biw
     * @param serviceClassName
     * @param serviceAddresses
     * @param latch 控制每个连接建立完毕才进行下一步处理
     */
    public void concurrentConnectServerByClass(Set<Class<?>> proxyClassList)
    {
        Registry registry = RegistryFactory.instance().getRegistry();
        
        // 用于阻塞等待所有代理类与服务端建立连接成功
        CountDownLatch proxyClassLatch = new CountDownLatch(proxyClassList.size());
        
        for (Class<?> proxyClass : proxyClassList)
        {
            List<String> serviceAddresses = registry.getServiceAddresses(proxyClass.getName());
            
            if (serviceAddresses.isEmpty())
            {
                proxyClassLatch.countDown();
            }
            else
            {
                threadPoolExecutor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        syncConnectServer(proxyClass.getName(), serviceAddresses, proxyClassLatch);
                    }
                });
            }
        }
        
        /** 阻塞等待所有连接建立成功 */
        try
        {
            proxyClassLatch.await();
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
    }
    
    /**
     * 服务方法调用时，若无该服务的链接，则调用建立连接
     * 
     * @param serviceClassName
     */
    public void syncConnectServer(String serviceClassName)
    {
        Registry registry = RegistryFactory.instance().getRegistry();
        List<String> serviceAddresses = registry.getServiceAddresses(serviceClassName);
        if (!serviceAddresses.isEmpty())
        {
            syncConnectServer(serviceClassName, serviceAddresses);
        }
        
    }
    
    /**
     * 加锁、加栅栏，针对一个服务，与已经注册该服务的服务端的建立连接
     * 
     * @Description
     * @author biw
     * @param serviceClassName
     * @param serviceAddresses
     * @param latch 控制每个连接建立完毕才进行下一步处理
     */
    public void syncConnectServer(String serviceClassName, List<String> serviceAddresses)
    {
        syncConnectServer(serviceClassName, serviceAddresses, null);
    }
    
    /**
     * 与已经注册该服务的服务端的建立连接
     * 若serviceClassLatch不为空，则阻塞等待，否则不阻塞
     * @author biw
     * @param serviceClassName
     * @param serviceAddresses
     * @param serviceClassLatch 用于阻塞等待所有代理类与服务端建立连接成功
     */
    public void syncConnectServer(String serviceClassName, List<String> serviceAddresses, CountDownLatch serviceClassLatch)
    {
        ReentrantLock connectServerLock = createAndGetLock(serviceClassName);
        try
        {
            connectServerLock.lock();
            
            List<String> noExistsServiceAddrList = getNoExistServiceAddr(serviceClassName, serviceAddresses);
            CountDownLatch latch = new CountDownLatch(noExistsServiceAddrList.size());
            for (String noExistServiceAddr : noExistsServiceAddrList)
            {
                doConnectServer(serviceClassName, noExistServiceAddr, latch);
            }
            
            latch.await();
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage(), e);
            throw new CrossException(e);
        }
        finally
        {
            if(serviceClassLatch != null)
            {
                serviceClassLatch.countDown();
            }
            
            connectServerLock.unlock();  
        }
    }
    
    private  ReentrantLock createAndGetLock(String serviceClassName)
    {
        if(connectServerLockMap.containsKey(serviceClassName))
        {
            return connectServerLockMap.get(serviceClassName);
        }
        
        ReentrantLock lock = new  ReentrantLock();
        connectServerLockMap.put(serviceClassName, lock);
        return lock;
    }
    
    /**
     * 筛选出未建立过连接的服务连接地址（不考虑当前恰巧被删除的服务连接地址）
     * 
     * @param serviceClassName
     * @param serviceAddresses
     * @return
     */
    private List<String> getNoExistServiceAddr(String serviceClassName, List<String> serviceAddresses)
    {
        ClientHandlerManager clienHandlerManager = getHandlerManager(serviceClassName);
        
        List<String> noExists = new ArrayList<>();
        for (String serviceAddr : serviceAddresses)
        {
            if (!clienHandlerManager.hasClientHandler(serviceAddr))
            {
                noExists.add(serviceAddr);
            }
        }
        
        return noExists;
    }
    
    public void doConnectServer(String serviceClassName, String serviceAddress, CountDownLatch latch)
    {
        threadPoolExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                EventLoopGroup group = new NioEventLoopGroup();
                try
                {
                    Bootstrap b = new Bootstrap();
                    b.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).handler(
                        new ChannelInitializer<SocketChannel>()
                        {
                            @Override
                            public void initChannel(SocketChannel ch)
                                throws Exception
                            {
                                ChannelPipeline cp = ch.pipeline();
                                
                                cp.addLast(new RpcEncoder());
                                cp.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 0));
                                cp.addLast(new RpcDecoder(RpcResponse.class));
                                cp.addLast(new ClientHandler());
                            }
                        });
                        
                    // Start the client.
                    String[] args = serviceAddress.split(":");
                    logger.debug("'netty connect to " + args[0] + ":" + args[1] + "for service " + serviceClassName);
                    ChannelFuture channelFuture = b.connect(args[0], Integer.parseInt(args[1])).sync();
                    
                    channelFuture.addListener(new ChannelFutureListener()
                    {
                        @Override
                        public void operationComplete(final ChannelFuture channelFuture)
                            throws Exception
                        {
                            if (channelFuture.isSuccess())
                            {
                                logger.debug("Successfully connect to remote server. remote peer = " + serviceAddress);
                                
                                /** 创建ClientHandler并且加入ClientHandlerManager */
                                ClientHandler clientHander =
                                    channelFuture.channel().pipeline().get(ClientHandler.class);
                                addHandler(serviceClassName, clientHander);
                                
                                /** 将客户端节点注册到zk服务节点下 */
                                Registry registry = RegistryFactory.instance().getRegistry();
                                registry.registClientForServer(serviceClassName, serviceAddress, CommonUtil.getServiceAddress(clientHander.getLocalPeer()));
                            }
                            else
                            {
                                logger.error("Failed connect to remote server. remote peer = " + serviceAddress);
                            }
                            
                            if (latch != null)
                            {
                                latch.countDown();
                            }
                        }
                    });
                    
                    // Wait until the connection is closed.
                    ChannelFuture closeChannelFuture = channelFuture.channel().closeFuture().sync();
                    
                    // 一旦连接关闭，需要将对应clientHandler从ConnectionManger中移除。
                    closeChannelFuture.addListener(new ChannelFutureListener()
                    {
                        @Override
                        public void operationComplete(ChannelFuture future)
                            throws Exception
                        {
                            if (!future.channel().isOpen())
                            {
                                ClientHandler clientHander =
                                    channelFuture.channel().pipeline().get(ClientHandler.class);
                                ConnectionManager.instance().rmHandler(serviceClassName, clientHander);
                            }
                        }
                    });
                }
                catch (Exception e)
                {
                    if (latch != null)
                    {
                        latch.countDown();
                    }
                    logger.error(e.getMessage());
                }
                finally
                {
                    group.shutdownGracefully();
                }
            }
        });
    }
    
    private void addHandler(String serviceClassName, ClientHandler clientHander)
    {
        if (!clientHandlerManagerMap.containsKey(serviceClassName))
        {
            ClientHandlerManager manager = new ClientHandlerManager(serviceClassName);
            clientHandlerManagerMap.put(serviceClassName, manager);
            manager.addClientHander(clientHander);
        }
        else
        {
            ClientHandlerManager manager = clientHandlerManagerMap.get(serviceClassName);
            manager.addClientHander(clientHander);
        }
    }
    
    private void rmHandler(String serviceClassName, ClientHandler clientHander)
    {
        if (clientHandlerManagerMap.containsKey(serviceClassName))
        {
            ClientHandlerManager manager = clientHandlerManagerMap.get(serviceClassName);
            manager.rmClientHander(clientHander);
        }
    }
    
    public void stop()
    {
        threadPoolExecutor.shutdown();
        
        // @TODO 遍历clientHander.close();clientHander.getChannel().close();
        
    }
}