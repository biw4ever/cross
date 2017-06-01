package com.yjz.cross.client.transport;

import java.lang.reflect.Proxy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.yjz.cross.client.proxy.IAsyncObjectProxy;
import com.yjz.cross.client.proxy.ObjectProxy;
import com.yjz.cross.client.registry.Registry;
import com.yjz.cross.client.registry.RegistryFactory;

/**
 * RPC Client(Create RPC proxy)
 * 
 * @author luxiaoxun
 */
public class CrossClient
{
    
    
    public CrossClient()
    {

    }
    
    @SuppressWarnings("unchecked")
    public <T> T create(Class<T> interfaceClass)
    {
        return (T)Proxy.newProxyInstance(interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass},
            new ObjectProxy<T>(interfaceClass));
    }
    
    public <T> IAsyncObjectProxy createAsync(Class<T> interfaceClass)
    {
        return new ObjectProxy<T>(interfaceClass);
    } 
}
