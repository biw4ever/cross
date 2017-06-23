package com.yjz.cross.server.transport;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.codec.RpcDecoder;
import com.yjz.cross.codec.RpcEncoder;
import com.yjz.cross.codec.RpcRequest;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class CrossServerBootStrap
{
    private static final Logger logger = LoggerFactory.getLogger(CrossServerBootStrap.class);
    
    private String serverAddress;
    
    private Map<String, Object> handlerMap;
    
    private Map<String, ThreadPoolExecutor> threadPoolExecutorMap;
    
    private CountDownLatch serverStartLatch;
    
    public CrossServerBootStrap(String serverAddress, Map<String, Object> handlerMap, Map<String, ThreadPoolExecutor> threadPoolExecutorMap, CountDownLatch serverStartLatch)
    {
        this.serverAddress = serverAddress;
        this.handlerMap = handlerMap;
        this.threadPoolExecutorMap = threadPoolExecutorMap;
        this.serverStartLatch = serverStartLatch;
    }
    
    public void bootStrap() throws Exception
    {

            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
               .option(ChannelOption.SO_BACKLOG, 100)
             .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                  @Override
                  public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                        p.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,0,4,0,0))
                        .addLast(new RpcDecoder(RpcRequest.class))
                        .addLast(new RpcEncoder())
                        .addLast(new ServerHandler(handlerMap, threadPoolExecutorMap));
                    }
                 });

            String[] array = serverAddress.split(":");
            String host = array[0];
            int port = Integer.parseInt(array[1]);

            ChannelFuture future = b.bind(host, port).sync();
            serverStartLatch.countDown();
            logger.info("Cross Server started on port "+ port);

            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
