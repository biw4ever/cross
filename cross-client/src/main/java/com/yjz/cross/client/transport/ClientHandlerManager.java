package com.yjz.cross.client.transport;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.CrossException;
import com.yjz.cross.client.init.CrossClientInitializer;
import com.yjz.cross.codec.RpcRequest;

/**
 * 客户端处理程序管理器
 * 
 * @ClassName ClientHandlerManager
 * @Description 客户端处理程序管理器负责将客户端请求发送给服务端并接收服务端的响应
 * @author biw
 * @Date 2017年5月18日 上午9:30:54
 * @version 1.0.0
 */
public class ClientHandlerManager
{
    private static final Logger logger = LoggerFactory.getLogger(ClientHandlerManager.class);
    
    private String serviceClassName = null;
    
    private CopyOnWriteArrayList<ClientHandler> clientHandlerList = new CopyOnWriteArrayList<ClientHandler>();
    
    private CopyOnWriteArraySet<InetSocketAddress> socketAddressSet = new CopyOnWriteArraySet<>();
    
    private AtomicInteger roundRobin = new AtomicInteger(0);
    
    public ClientHandlerManager(String serviceClassName)
    {
        this.serviceClassName = serviceClassName;
    }
    
    public boolean hasClientHandler()
    {
        return !clientHandlerList.isEmpty();
    }
    
    public boolean hasClientHandler(InetSocketAddress socketAddress)
    {
        return socketAddressSet.contains(socketAddress);
    }
    
    public boolean hasClientHandler(String serviceAddress)
    {
        String[] parts = serviceAddress.split(":");
        InetSocketAddress socketAddr = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
        return socketAddressSet.contains(socketAddr);
    }
    
    public void addClientHander(ClientHandler clientHander)
    {
        clientHandlerList.add(clientHander);
        socketAddressSet.add((InetSocketAddress)clientHander.getRemotePeer());
    }
    
    public void rmClientHander(ClientHandler clientHander)
    {
        clientHandlerList.remove(clientHander);
        socketAddressSet.remove((InetSocketAddress)clientHander.getRemotePeer());
        clientHander.close();
    }
    
    public void clearClientHandlers()
    {
        clientHandlerList.clear();
        socketAddressSet.clear();
        
        for (ClientHandler clientHander : clientHandlerList)
        {
            clientHander.close();
        }
    }
    
    public RPCFuture sendRequest(RpcRequest request)
    {
        connectServerIfNeed(request.getClassName());
        
        try
        {
            ClientHandler clientHandler = this.chooseClientHandler();
            
            InetSocketAddress socketAddr = (InetSocketAddress)clientHandler.getRemotePeer();
            logger.debug("Calling remoter service at {} ", socketAddr.getHostName() + ":" + socketAddr.getPort());
            return clientHandler.sendRequest(request);
        }
        catch (IndexOutOfBoundsException e)
        {
            logger.error(e.getMessage(), e);
            throw new CrossException(e);
        }
    }
    
    public void connectServerIfNeed(String serviceClassName)
    {
        if (clientHandlerList.isEmpty())
        {
            String message = "there is no client handler for " + serviceClassName
                + ", perhaps there is not available service on the registry! Now try to find servcies and connect to them.";
            logger.debug(message);
            
            ConnectionManager.instance().syncConnectServer(serviceClassName);
        }
    }
    
    private ClientHandler chooseClientHandler()
    {
        if(clientHandlerList.isEmpty())
        {
            String message = "there is still no client handler for " + serviceClassName
                + "! ";
            logger.debug(message);
            throw new CrossException(message);
        }
        
        int size = clientHandlerList.size();
        int index = roundRobin.getAndIncrement() % size;
        if (roundRobin.intValue() >= size)
        {
            roundRobin.set(0);
        }
        
        return clientHandlerList.get(index);
    }
    
    public void updateClientHandler(List<String> addressList)
    {
        List<InetSocketAddress> socketAddrList = convert(addressList);
        
        addNewClientHandlers(socketAddrList);
        
        rmNonExsitedClientHandlers(socketAddrList);
    }
    
    private List<InetSocketAddress> convert(List<String> addressList)
    {
        List<InetSocketAddress> socketAddrList = new ArrayList<>();
        for (String address : addressList)
        {
            String[] parts = address.split(":");
            if (parts.length == 2)
            {
                InetSocketAddress socketAddress = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
                socketAddrList.add(socketAddress);
            }
        }
        
        return socketAddrList;
    }
    
    /**
     * 从服务下所有结点中查找未连接的进行连接
     * 
     * @param socketAddrList
     */
    private void addNewClientHandlers(List<InetSocketAddress> socketAddrList)
    {
        List<InetSocketAddress> newSocketAddrList = new ArrayList<>();
        for (InetSocketAddress socketAddr : socketAddrList)
        {
            if (!socketAddressSet.contains(socketAddr))
            {
                newSocketAddrList.add(socketAddr);
            }
        }
        ConnectionManager.instance().connectServer(serviceClassName, newSocketAddrList);
    }
    
    private void rmNonExsitedClientHandlers(List<InetSocketAddress> socketAddrList)
    {
        for (ClientHandler clientHandler : clientHandlerList)
        {
            if (!socketAddrList.contains((InetSocketAddress)clientHandler.getRemotePeer()))
            {
                socketAddressSet.remove((InetSocketAddress)clientHandler.getRemotePeer());
                clientHandlerList.remove(clientHandler);
                clientHandler.close();
            }
        }
    }
    
}
