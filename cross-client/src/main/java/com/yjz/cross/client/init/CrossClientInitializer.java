package com.yjz.cross.client.init;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import com.yjz.cross.CrossException;
import com.yjz.cross.client.annotation.CrossReference;
import com.yjz.cross.client.registry.Registry;
import com.yjz.cross.client.registry.RegistryFactory;
import com.yjz.cross.client.transport.ConnectionManager;
import com.yjz.cross.client.transport.CrossClient;
import com.yjz.cross.client.util.AopTargetUtil;
import com.yjz.cross.config.Configuration;

/**
 * 
 * @ClassName CrossClientInitializer
 * @Description 伴随Spring上下文初始化，初始化Cross客户端
 * @author biw
 * @Date 2017年5月18日 下午1:32:48
 * @version 1.0.0
 */
@Component
public class CrossClientInitializer implements ApplicationContextAware
{
    private static final Logger logger = LoggerFactory.getLogger(CrossClientInitializer.class);
    
    public static Configuration CONFIGURATION;
    
    public static ApplicationContext APPLICATION_CONTEXT;
    
    @Value("#{cross['zk.address']}")
    private String zkAddress;
    
    public static void bootStrap(List<Object> objs, Configuration conf)
        throws BeansException
    {
        CONFIGURATION = conf;
        
        // 遍历所有对象，将其中标注了CrossReference的字段替换成代理对象，此代理对象的方法调用将通过Rpc的方式调用服务端来实现
        Set<Class<?>> proxyClassList = new HashSet<>();
        for (Object prxyObj : objs)
        {
            try
            {
                Object obj = AopTargetUtil.getTarget(prxyObj);
                Field[] fields = obj.getClass().getDeclaredFields();
                for (Field field : fields)
                {
                    CrossReference annotation = field.getAnnotation(CrossReference.class);
                    if (annotation != null)
                    {
                        Class<?> proxyClass = field.getType();
                        // String registryName = annotation.registryName();
                        
                        // 生成Field的代理对象
                        proxyReferences(obj, field, proxyClass);
                        
                        // 收集代理类，去重后建立每个代理类与Service服务端之间的连接
                        proxyClassList.add(proxyClass);
                    }
                }
            }
            catch (Exception e)
            {
                logger.error(e.getMessage());
            }  
        }
        
        // 将所有代理服务知会Registry
        Registry registry = RegistryFactory.instance().getRegistry();
        registry.setServiceClass(proxyClassList);
 
        // 建立每个代理类与Service的服务端的连接   
        connectServer(proxyClassList);
        
        // 将已连接的客户端和服务端注册到cross-client根节点下
        registry.registClientRoot();
        
        // Watch ServerRoot Node for service updates and services for address update
        registry.watchServerRootAndServices();
    }
    
    private static void proxyReferences(Object obj, Field field, Class<?> proxyClass)
    {
        try
        {
            ReflectionUtils.makeAccessible(field);
            // @TODO new MockUp
            field.set(obj, getFieldProxy(proxyClass));
        }
        catch (IllegalArgumentException | IllegalAccessException e)
        {
            String message = "create proxy for " + obj.getClass().getName() + "." + field.getName() + " failed'!";
            logger.error(message, e);
            throw new CrossException(e);
        }
    }
    
    private static <T> T getFieldProxy(Class<T> clazz)
    {
        CrossClient crossClient = new CrossClient();
        return crossClient.create(clazz);
    }
    
    public static void connectServer(Set<Class<?>> proxyClassList)
    {
        ConnectionManager.instance().concurrentConnectServerByClass(proxyClassList);
    }
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
        throws BeansException
    {
        APPLICATION_CONTEXT = applicationContext;
        
        Configuration conf = new Configuration();
        conf.addRegistry(zkAddress);
        
        // 遍历容器中所有Controller和Service，获取其中标注了CrossReference的字段，将其替换成代理对象，此代理对象的方法调用将通过Rpc的方式调用服务端来实现 
        logger.info("Start loading Cross references");
        
        String[] beanNames = applicationContext.getBeanDefinitionNames();
        List<Object> beanObjectList = new ArrayList<>();
        for (String beanName : beanNames)
        {
            Object obj = applicationContext.getBean(beanName);
            beanObjectList.add(obj);
        }
        
        CrossClientInitializer.bootStrap(beanObjectList, conf);
        
        logger.info("Fininshed loading Cross references");
    }
    
}
