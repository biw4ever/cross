package com.yjz.cross.client.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @ClassName CrossReference
 * @Description Cross客户端对服务端的引用
 * @author biw
 * @Date 2017年5月18日 下午1:29:32
 * @version 1.0.0
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CrossReference
{
    String registryName() default "default";
}
