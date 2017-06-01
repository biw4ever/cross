package com.yjz.cross.client.transport;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
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
    
    private ReentrantLock connectServerLock = new ReentrantLock();
    
    private ConnectionManager()
    {
    }
    
    public static ConnectionManager instance()
    {
        return connManager;
    }
    
    public Map<String, ClientHandlerManager>getHandlerManagers()
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
     * 针对每一个服务，与已经注册该服务的服务端的建立连接
     * 
     * @Description
     * @author biw
     * @param serviceClassName
     * @param serviceAddresses
     */
    public void connectServer(String serviceClassName, List<String> serviceAddresses)
    {
        try
        {
            connectServerLock.lock();
            
            ClientHandlerManager clienHandlerManager = getHandlerManager(serviceClassName);
            if (clienHandlerManager.hasClientHandler())
            {
                return;
            }
            
            for (String serviceAddress : serviceAddresses)
            {
                connectServer(serviceClassName, serviceAddress, null);
            }
            
            Registry registry = RegistryFactory.instance().getRegistry();
            registry.watchService(serviceClassName);
        }
        finally
        {
            connectServerLock.unlock();
        }
        
    }
    
    /**
     * 针对每一个服务，与已经注册该服务的服务端的建立连接
     * 
     * @Description
     * @author biw
     * @param serviceClassName
     * @param serviceAddresses
     * @param latch 控制每个连接建立完毕才进行下一步处理
     */
    public void syncConnectServer(String serviceClassName, List<String> serviceAddresses)
    {
        try
        {
            connectServerLock.lock();
            
            ClientHandlerManager clienHandlerManager = getHandlerManager(serviceClassName);
            if (clienHandlerManager.hasClientHandler())
            {
                return;
            }
            
            CountDownLatch latch = new CountDownLatch(serviceAddresses.size());
            for (String serviceAddress : serviceAddresses)
            {
                connectServer(serviceClassName, serviceAddress, latch);
            }
            latch.await();
            
            Registry registry = RegistryFactory.instance().getRegistry();
            registry.watchService(serviceClassName);
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage(), e);
            throw new CrossException(e);
        }
        finally
        {
            connectServerLock.unlock();
        }
    }
    
    public void connectServer(String serviceClassName, InetSocketAddress socketAddress)
    {
        String serviceAddress = socketAddress.getHostName() + ":" + socketAddress.getPort();
        try
        {
            connectServerLock.lock();
            
            ClientHandlerManager clienHandlerManager = getHandlerManager(serviceClassName);
            if(clienHandlerManager.hasClientHandler())
            {
                return;
            }
            
            connectServer(serviceClassName, serviceAddress, null);
        }
        finally
        {
            connectServerLock.unlock();
        }
    }
    
    public void connectServer(String serviceClassName, String serviceAddress, CountDownLatch latch)
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
                                cp.addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 0));
                                cp.addLast(new RpcDecoder(RpcResponse.class));
                                cp.addLast(new ClientHandler());
                            }
                        });
                        
                    // Start the client.
                    String[] args = serviceAddress.split(":");
                    System.out.println("'netty connect to " + args[0] + ":" + args[1]);
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
                                ClientHandler clientHander =
                                    channelFuture.channel().pipeline().get(ClientHandler.class);
                                addHandler(serviceClassName, clientHander);
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
                    logger.error(e.getMessage(), e);
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