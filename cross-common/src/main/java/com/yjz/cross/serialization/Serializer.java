package com.yjz.cross.serialization;

public interface Serializer
{
    public Object deserialize(byte[] bytes, Class<?> clazz);
    
    public byte[] serialize(Object obj, Class<?> clazz);
}
