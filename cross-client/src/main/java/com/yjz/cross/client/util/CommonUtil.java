package com.yjz.cross.client.util;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class CommonUtil
{
    public static String getServiceAddress(SocketAddress socketAddress)
    {
        InetSocketAddress sa = (InetSocketAddress) socketAddress;
        return sa.getAddress() + ":" + sa.getPort();
    }
}
