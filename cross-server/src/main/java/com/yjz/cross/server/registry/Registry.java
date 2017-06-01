package com.yjz.cross.server.registry;

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
     * @Description 注册服务到注册中心
     * @author biw
     * @param serviceName
     */
    public void regist(String serviceName);
}
