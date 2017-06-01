package com.yjz.cross.server.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * @ClassName CrossService
 * @Description CrossService注解定义，对应Rpc类
 * @author biw
 * @Date 2017年5月16日 下午9:01:04
 * @version 1.0.0
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CrossService
{
    String registryName() default "default";  
}
