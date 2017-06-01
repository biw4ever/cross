package com.yjz.cross.serialization;

import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;

public class ProtoStaffSerializer implements Serializer
{
    private Schema<?> cachedSchema;
    
    public ProtoStaffSerializer()
    {
        
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Object deserialize(byte[] bytes, Class<?> clazz)
    {
        Objenesis objenesis = new ObjenesisStd(true);
        
        try {
            Object message = objenesis.newInstance(clazz);
            Schema schema = getSchema(clazz);
            ProtostuffIOUtil.mergeFrom(bytes, message, schema);
            return message;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public byte[] serialize(Object obj, Class<?> clazz)
    {
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        try {
            Schema schema = getSchema(clazz);
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
    }
    
    @SuppressWarnings("rawtypes")
    private Schema getSchema(Class<?> clazz) {
        
        if (cachedSchema == null) {
            cachedSchema = RuntimeSchema.createFrom(clazz);
        }
        return cachedSchema;
    }  
}
