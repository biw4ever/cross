package com.yjz.cross.client.proxy;

import com.yjz.cross.client.transport.RPCFuture;

/**
 * 
 * @ClassName IAsyncObjectProxy
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月18日 上午10:16:07
 * @version 1.0.0
 */
public interface IAsyncObjectProxy
{
    public RPCFuture call(String funcName, Object... args);
}