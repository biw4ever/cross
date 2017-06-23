package com.yjz.cross.server.registry;

import java.util.Set;

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
     * @Description 通知registry哪些服务需要注册
     * @author biw
     * @param serviceNameList
     */
    public void addServiceName(String serviceName);
    
    /**
     * @Description 注册服务到注册中心
     * @author biw
     */
    public void registServices();
}
