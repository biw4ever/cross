package com.yjz.cross.config;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName Configuration
 * @Description Cross的配置信息
 * @author biw
 * @Date 2017年5月19日 上午8:48:49
 * @version 1.0.0
 */
public class Configuration
{
    protected Map<String, String> registryMap = new HashMap<>();
    
    protected String serverAddress;
    
    public Map<String, String> getRegistryMap()
    {
        return registryMap;
    }

    public void setRegistryMap(Map<String, String> registryMap)
    {
        this.registryMap = registryMap;
    }

    public String getServerAddress()
    {
        return serverAddress;
    }

    public void setServerAddress(String serverIpAddress)
    {
        this.serverAddress = serverIpAddress;
    }

    public void addRegistry(String registryName, String registryAddress)
    {
        registryMap.put(registryName, registryAddress);
    }
    
    public void addRegistry(String registryAddress)
    {
        addRegistry("default", registryAddress);
    }
    
    public String getRegistryAddress(String registryName)
    {
        return registryMap.get(registryName);
    }
    
   
}
