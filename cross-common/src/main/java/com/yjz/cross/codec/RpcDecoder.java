package com.yjz.cross.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

import com.yjz.cross.serialization.Serializer;
import com.yjz.cross.serialization.SerializerFactory;

/**
 * 
 * @ClassName RpcDecoder
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月16日 下午10:01:39
 * @version 1.0.0
 */
public class RpcDecoder extends ByteToMessageDecoder {

    private Class<?> genericClass;

    public RpcDecoder(Class<?> genericClass) {
        this.genericClass = genericClass;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4) {
            return;
        }
        in.markReaderIndex();
        int dataLength = in.readInt();
        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] data = new byte[dataLength];
        in.readBytes(data);

        Serializer serializer = SerializerFactory.instance().getSerializer();
        Object obj = serializer.deserialize(data, genericClass);
        out.add(obj);
    }

}
