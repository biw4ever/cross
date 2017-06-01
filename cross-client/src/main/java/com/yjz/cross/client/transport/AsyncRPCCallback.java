package com.yjz.cross.client.transport;

/**
 * Created by luxiaoxun on 2016-03-17.
 */
public interface AsyncRPCCallback {

    void success(Object result);

    void fail(Exception e);

}
