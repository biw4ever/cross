package com.yjz.cross.server.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * @ClassName CrossMethod
 * @Description CrossMethod注解定义，对应Rpc方法
 * @author biw
 * @Date 2017年5月16日 下午9:26:08
 * @version 1.0.0
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CrossMethod
{
    String value();
}
