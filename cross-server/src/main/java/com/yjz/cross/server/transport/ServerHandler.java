package com.yjz.cross.server.transport;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.codec.RpcRequest;
import com.yjz.cross.codec.RpcResponse;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;

/**
 * 
 * @ClassName RpcHandler
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月16日 下午11:01:58
 * @version 1.0.0
 */
public class ServerHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);

    private final Map<String, Object> handlerMap;
    
    private final Map<String, ThreadPoolExecutor> threadPoolExecutorMap;

    public ServerHandler(Map<String, Object> handlerMap, Map<String, ThreadPoolExecutor> threadPoolExecutorMap) {
        this.handlerMap = handlerMap;
        this.threadPoolExecutorMap = threadPoolExecutorMap;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx,final RpcRequest request) throws Exception {
        this.execute(request.getClassName(), new Runnable() {
            @Override
            public void run() {
                LOGGER.debug("Receive request " + request.getRequestId());
                RpcResponse response = new RpcResponse();
                response.setRequestId(request.getRequestId());
                try {
                    Object result = handle(request);
                    response.setResult(result);
                } catch (Throwable t) {
                    response.setError(t.toString());
                    LOGGER.error("RPC Server handle request error",t);
                }
                ctx.writeAndFlush(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                         
                        LOGGER.debug("Send response for request " + request.getRequestId());
                    }
                });
            }
        });
    }

    private Object handle(RpcRequest request) throws Throwable {
        String className = request.getClassName();
        Object serviceBean = handlerMap.get(className);

        Class<?> serviceClass = serviceBean.getClass();
        String methodName = request.getMethodName();
        Class<?>[] parameterTypes = request.getParameterTypes();
        Object[] parameters = request.getParameters();

        LOGGER.debug(serviceClass.getName());
        LOGGER.debug(methodName);
        for (int i = 0; i < parameterTypes.length; ++i) {
            LOGGER.debug(parameterTypes[i].getName());
        }
        if (parameters != null)
        {
            for (int i = 0; i < parameters.length; ++i)
            {
                LOGGER.debug(parameters[i].toString());
            }
        }

        // JDK reflect
        /*Method method = serviceClass.getMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(serviceBean, parameters);*/

        // Cglib reflect
        FastClass serviceFastClass = FastClass.create(serviceClass);
        FastMethod serviceFastMethod = serviceFastClass.getMethod(methodName, parameterTypes);
        return serviceFastMethod.invoke(serviceBean, parameters);
//        Map<String, ?> map  = CrossServerInitializer.APPLICATIONCONTEXT.getBeansOfType(Class.forName(className));
//        if(!map.isEmpty())
//        {
//            Object beanObj = map.values().iterator().next();
//            Method method = serviceClass.getMethod(methodName, parameterTypes);
//            method.setAccessible(true);
//            return method.invoke(beanObj, parameters);
//        }
//        
//        LOGGER.error("there is no bean corresponding to " + className);
//        return null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error("server caught exception", cause);
        ctx.close();
    }
    
    public void execute(String serviceName, Runnable task)
    {
        ThreadPoolExecutor threadPoolExecutor = threadPoolExecutorMap.get(serviceName);
        threadPoolExecutor.execute(task);
    }
    
    
}
