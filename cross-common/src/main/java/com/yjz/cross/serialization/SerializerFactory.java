package com.yjz.cross.serialization;

public class SerializerFactory
{
    private static volatile SerializerFactory factory;
    
    private SerializerFactory()
    {
        
    }
    
    @SuppressWarnings("unchecked")
    public static SerializerFactory instance()
    {
        if(factory == null)
        {
            factory =  new SerializerFactory();
        }  
        
        return (SerializerFactory) factory;
    }
    
    
    public Serializer getSerializer()
    {
        Serializer serializer = new ProtoStaffSerializer();
        return serializer;
    }
}
