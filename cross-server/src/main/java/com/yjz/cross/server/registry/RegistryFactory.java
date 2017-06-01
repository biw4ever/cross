package com.yjz.cross.server.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName RegistryFactory
 * @Description 根据系统配置获取注册中心实现（例如ZkRegistry）
 * @author biw
 * @Date 2017年5月16日 02:58:59
 * @version 1.0.0
 */
public class RegistryFactory
{
    private static volatile RegistryFactory factory = null;
    
    private static Map<String, Registry> registryMap = new ConcurrentHashMap<>();
    
    private RegistryFactory()
    {
        
    }
    
    /**
     * @Description 获取注册中心工厂单例
     * @author biw
     * @return
     */
    public static RegistryFactory instance()
    {
        if (factory == null)
        {
            synchronized (RegistryFactory.class)
            {
                if (factory == null)
                {
                    factory = new RegistryFactory();
                }
            }
            
        }
        
        return factory;
    }
    
    /**
     * @Description 获取对应名称的注册中心
     * @author biw
     * @param registryName
     * @return
     */
    public Registry getRegistry(String registryName)
    {
        if (registryMap.containsKey(registryName))
        {
            return registryMap.get(registryName);
        }
        
        Registry registry = new ZkRegistry(registryName);
        registryMap.put(registryName, registry);
        return registry;
    }
    
    /**
     * @Description (获取默认注册中心)
     * @author biw
     * @return
     */
    public Registry getRegistry()
    {
        return getRegistry("default");
    }
}
