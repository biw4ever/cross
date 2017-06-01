package com.yjz.cross.client.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.client.transport.ClientHandlerManager;
import com.yjz.cross.client.transport.ConnectionManager;
import com.yjz.cross.client.transport.RPCFuture;
import com.yjz.cross.codec.RpcRequest;

/**
 * 
 * @ClassName ObjectProxy
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月18日 上午10:16:15
 * @version 1.0.0
 * @param <T>
 */
public class ObjectProxy<T> implements InvocationHandler, IAsyncObjectProxy
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectProxy.class);
    
    private Class<T> clazz;
    
    public ObjectProxy(Class<T> clazz)
    {
        this.clazz = clazz;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable
    {
        if (Object.class == method.getDeclaringClass())
        {
            String name = method.getName();
            if ("equals".equals(name))
            {
                return proxy == args[0];
            }
            else if ("hashCode".equals(name))
            {
                return System.identityHashCode(proxy);
            }
            else if ("toString".equals(name))
            {
                return proxy.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(proxy))
                    + ", with InvocationHandler " + this;
            }
            else
            {
                throw new IllegalStateException(String.valueOf(method));
            }
        }
        
        RpcRequest request = new RpcRequest();
        request.setRequestId(UUID.randomUUID().toString());
        request.setClassName(method.getDeclaringClass().getName());
        request.setMethodName(method.getName());
        request.setParameterTypes(method.getParameterTypes());
        request.setParameters(args);
        // Debug
        LOGGER.debug(method.getDeclaringClass().getName());
        LOGGER.debug(method.getName());
        for (int i = 0; i < method.getParameterTypes().length; ++i)
        {
            LOGGER.debug(method.getParameterTypes()[i].getName());
        }
        
        if(args != null)
        {
            for (int i = 0; i < args.length; ++i)
            {
                LOGGER.debug(args[i].toString());
            }
        }
        
        ClientHandlerManager handlerManager = ConnectionManager.instance().getHandlerManager(request.getClassName());
        RPCFuture rpcFuture = handlerManager.sendRequest(request);
        return rpcFuture.get(10, TimeUnit.SECONDS);  // 最多等待10秒，没有结果则抛异常
    }
    
    @Override
    public RPCFuture call(String funcName, Object... args)
    {
        RpcRequest request = createRequest(this.clazz.getName(), funcName, args);
        
        ClientHandlerManager handlerManager = ConnectionManager.instance().getHandlerManager(request.getClassName());
        RPCFuture rpcFuture = handlerManager.sendRequest(request);
        return rpcFuture;
    }
    
    private RpcRequest createRequest(String className, String methodName, Object[] args)
    {
        RpcRequest request = new RpcRequest();
        request.setRequestId(UUID.randomUUID().toString());
        request.setClassName(className);
        request.setMethodName(methodName);
        request.setParameters(args);
        
        Class<?>[] parameterTypes = new Class<?>[args.length];
        
        // Get the right class type
        for (int i = 0; i < args.length; i++)
        {
            parameterTypes[i] = getClassType(args[i]);
        }
        request.setParameterTypes(parameterTypes);
       
        LOGGER.debug(className);
        LOGGER.debug(methodName);
        for (int i = 0; i < parameterTypes.length; ++i)
        {
            LOGGER.debug(parameterTypes[i].getName());
        }
        for (int i = 0; i < args.length; ++i)
        {
            LOGGER.debug(args[i].toString());
        }
        
        return request;
    }
    
    /**
     * TODO 解析出所有的参数类型
     * @Description (TODO这里用一句话描述这个方法的作用)
     * @author biw
     * @param obj
     * @return
     */
    private Class<?> getClassType(Object obj)
    {
        return obj.getClass();
//        Class<?> classType = obj.getClass();
//        String typeName = classType.getName();
//        switch (typeName)
//        {
//            case "java.lang.Integer":
//                return Integer.TYPE;
//            case "java.lang.Long":
//                return Long.TYPE;
//            case "java.lang.Float":
//                return Float.TYPE;
//            case "java.lang.Double":
//                return Double.TYPE;
//            case "java.lang.Character":
//                return Character.TYPE;
//            case "java.lang.Boolean":
//                return Boolean.TYPE;
//            case "java.lang.Short":
//                return Short.TYPE;
//            case "java.lang.Byte":
//                return Byte.TYPE;
//        }
//        
//        return classType;
    }
    
}
