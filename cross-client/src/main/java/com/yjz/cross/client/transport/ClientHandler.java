package com.yjz.cross.client.transport;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.codec.RpcRequest;
import com.yjz.cross.codec.RpcResponse;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 
 * @ClassName RpcClientHandler
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月17日 下午1:50:21
 * @version 1.0.0
 */
public class ClientHandler extends SimpleChannelInboundHandler<RpcResponse>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientHandler.class);
    
    private ConcurrentHashMap<String, RPCFuture> pendingRPC = new ConcurrentHashMap<>();
    
    private volatile Channel channel;
    
    private SocketAddress remotePeer;
    
    private ClientHandlerManager clientHandlerManager;
    
    public void setClientHandlerManager(ClientHandlerManager clientHandlerManager)
    {
        this.clientHandlerManager = clientHandlerManager;
    }
    
    public Channel getChannel()
    {
        return channel;
    }
    
    public SocketAddress getRemotePeer()
    {
        return remotePeer;
    }
    
    public SocketAddress getLocalPeer()
    {
        return channel.localAddress();
    }
    
    @Override
    public void channelActive(ChannelHandlerContext ctx)
        throws Exception
    {
        super.channelActive(ctx);
        this.remotePeer = this.channel.remoteAddress();
    }
    
    @Override
    public void channelRegistered(ChannelHandlerContext ctx)
        throws Exception
    {
        super.channelRegistered(ctx);
        this.channel = ctx.channel();
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, RpcResponse response)
        throws Exception
    {
        String requestId = response.getRequestId();
        RPCFuture rpcFuture = pendingRPC.get(requestId);
        if (rpcFuture != null)
        {
            pendingRPC.remove(requestId);
            rpcFuture.done(response);
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception
    {
        LOGGER.error(cause.getMessage() + remotePeer.toString());
        ctx.close();
        this.clientHandlerManager.rmClientHander(this);
    }
    
    public void close()
    {
        if(channel.isActive())
        {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
    
    public RPCFuture sendRequest(RpcRequest request)
    {
        RPCFuture rpcFuture = new RPCFuture(request);
        pendingRPC.put(request.getRequestId(), rpcFuture);
        channel.writeAndFlush(request);
        
        return rpcFuture;
    }
    
    /**
     * @Description 返回ClientHandler使用的Channel是否处于正常连接中
     * @author biw
     * @return
     */
    public boolean isActive()
    {
        return channel.isActive();
    }
    
    public String getServiceClassName()
    {
        return clientHandlerManager.getServiceClassName();
    }
}
