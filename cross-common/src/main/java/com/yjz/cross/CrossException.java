package com.yjz.cross;

/**
 * @ClassName CrossException
 * @Description 自定义异常
 * @author biw
 * @Date 2017年5月16日 下午3:52:58
 * @version 1.0.0
 */
public class CrossException extends RuntimeException
{
    public CrossException(Throwable t)
    {
        super(t);
    }
    
    public CrossException(String message)
    {
        super(message);
    }
}
