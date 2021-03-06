package com.yjz.cross.server.init;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.yjz.cross.CrossException;
import com.yjz.cross.config.Configuration;
import com.yjz.cross.server.annotation.CrossService;
import com.yjz.cross.server.registry.Registry;
import com.yjz.cross.server.registry.RegistryFactory;
import com.yjz.cross.server.transport.CrossServerBootStrap;

/**
 * @ClassName CrossServer上下文初始化
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月16日 下午9:38:05
 * @version 1.0.0
 */
@Component
public class CrossServerInitializer implements ApplicationContextAware
{
    private static final Logger logger = LoggerFactory.getLogger(CrossServerInitializer.class);
    
    @Value("#{cross['cross.server.address']}")
    private String serverAddress;
    
    @Value("#{cross['zk.address']}")
    private String zkAddress;
    
    public static Configuration CONFIGURATION;
    
    private static final Map<String, Object> handlerMap = new HashMap<String, Object>();
    
    private static final Map<String, ThreadPoolExecutor> threadPoolExecutorMap =
        new HashMap<String, ThreadPoolExecutor>();
    
    public static ApplicationContext APPLICATIONCONTEXT;
    
    public static void bootStrap(Object[] objs, Configuration conf)
    {
        CONFIGURATION = conf;
        
        Registry registry = RegistryFactory.instance().getRegistry();
        
        for (Object obj : objs)
        {
            // 暂不实现多注册中心
            // CrossService annotation = obj.getClass().getAnnotation(CrossService.class);
            // String registryName = annotation.registryName();
            // Registry registry = RegistryFactory.instance().getRegistry(registryName);
            
            /** 把服务通知给Registry */
            Class<?> clazz = obj.getClass().getInterfaces()[0];
            registry.addServiceName(clazz.getName());
            
            /** 获取该服务的处理线程池 */
            ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(16, 16, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
            threadPoolExecutorMap.put(clazz.getName(), threadPoolExecutor);
            handlerMap.put(clazz.getName(), obj);
        }
        
        /** 启动netty服务端等待客户端连接和访问 */
        bootServer();
        
        /** 再连接Zk，将服务注册上Zk */
        registry.registServices();
        
    }
    
    private static void bootServer()
    {
        CountDownLatch serverStartLatch = new CountDownLatch(1);
        
        try
        {
            ExecutorService serverThreadExecutor = Executors.newSingleThreadExecutor();
            serverThreadExecutor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    CrossServerBootStrap bootStrap = new CrossServerBootStrap(CONFIGURATION.getServerAddress(),
                        handlerMap, threadPoolExecutorMap, serverStartLatch);
                    try
                    {
                        bootStrap.bootStrap();
                    }
                    catch (Exception e)
                    {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
            
            serverStartLatch.await();
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage());
        }
    }
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
        throws BeansException
    {
        try
        {
            APPLICATIONCONTEXT = applicationContext;
            
            // 获取标注了CrossService的所有SpringBean对象
            logger.info("Start loading Cross Services.");
            Map<String, Object> crossServiceMap = applicationContext.getBeansWithAnnotation(CrossService.class);
            Object[] objects = crossServiceMap.values().toArray();
            
            Configuration conf = new Configuration();
            conf.addRegistry(zkAddress.trim());
            conf.setServerAddress(serverAddress.trim());
            
            CrossServerInitializer.bootStrap(objects, conf);
            logger.info("Fininshed loading Cross Services.");
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(), e);
            throw new CrossException(e);
        }
        
    }
}
