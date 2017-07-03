package com.yjz.cross.client.util;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yjz.cross.client.transport.ClientHandler;

public class CommonUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtil.class);
    
    public static String getServiceAddress(SocketAddress socketAddress)
    {
        InetSocketAddress sa = (InetSocketAddress) socketAddress;
        return sa.getAddress().getHostAddress()+ ":" + sa.getPort();
    }
    
    public static String convertObj2Json(Object obj)
    {     
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(obj);
        }
        catch (JsonProcessingException e)
        {
            LOGGER.error(e.getMessage());
            return "";
        }
    }
}
