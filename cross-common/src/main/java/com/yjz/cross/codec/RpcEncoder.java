package com.yjz.cross.codec;

import com.yjz.cross.serialization.Serializer;
import com.yjz.cross.serialization.SerializerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 
 * @ClassName RpcEncoder
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月16日 下午10:01:59
 * @version 1.0.0
 */
public class RpcEncoder extends MessageToByteEncoder
{  
    @Override
    public void encode(ChannelHandlerContext ctx, Object in, ByteBuf out)
        throws Exception
    {
        Serializer serializer = (Serializer) SerializerFactory.instance().getSerializer();
        byte[] data = serializer.serialize(in, in.getClass());
        out.writeInt(data.length);
        out.writeBytes(data);
    }
}
