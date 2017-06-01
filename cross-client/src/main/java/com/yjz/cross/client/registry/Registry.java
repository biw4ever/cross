package com.yjz.cross.client.registry;

import java.util.List;

/**
 * @ClassName Registry
 * @Description
 * @author biw
 * @Date 2017年5月16日 02:47:56
 * @version 1.0.0
 */
public interface Registry
{
    /**
     * @Description 从注册中心后去服务地址  ip:port
     * @author biw
     * @param serviceName
     */
    public List<String> getServiceAddresses(String serviceClassName);
    
    /**
     * @Description 检查服务是否发生变，变更则更新对应ClientHandlerManager
     * @author biw
     */
    public void watchService(String serviceClassName);
    
    /**
     * @Description 检查根节点下服务是否有变更，变更则更新对应ClientHandlerManager
     * @author biw
     */
    public void watchRoot();
    
    /** 
     * 关闭注册中心
     * @Description (TODO这里用一句话描述这个方法的作用)
     * @author biw
     */
    public void stop();
}