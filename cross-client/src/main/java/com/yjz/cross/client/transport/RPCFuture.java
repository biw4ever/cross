package com.yjz.cross.client.transport;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yjz.cross.client.init.CrossClientInitializer;
import com.yjz.cross.client.service.AccessLogService;
import com.yjz.cross.client.util.CommonUtil;
import com.yjz.cross.codec.RpcRequest;
import com.yjz.cross.codec.RpcResponse;
import com.yjz.cross.monitor.pojo.AccessLog;

/**
 * 
 * @ClassName RPCFuture
 * @Description TODO(这里用一句话描述这个类的作用)
 * @author biw
 * @Date 2017年5月18日 上午10:16:23
 * @version 1.0.0
 */
public class RPCFuture implements Future<Object>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RPCFuture.class);
    
    private Sync sync;
    
    private RpcRequest request;
    
    private RpcResponse response;
    
    private SocketAddress localAddr;
    
    private SocketAddress remoteAddr;
    
    private long startTime;
    
    private long responseTimeThreshold = 5000;
    
    private List<AsyncRPCCallback> pendingCallbacks = new ArrayList<AsyncRPCCallback>();
    
    private ReentrantLock lock = new ReentrantLock();
    
    // 负责向服务端发请求的线程池
    private static ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(16, 16, 600L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(65536));
    
    public RPCFuture(RpcRequest request, SocketAddress localAddr, SocketAddress remoteAddr)
    {
        this.sync = new Sync();
        this.request = request;
        this.localAddr = localAddr;
        this.remoteAddr = remoteAddr;
        this.startTime = System.currentTimeMillis();
    }
    
    @Override
    public boolean isDone()
    {
        return sync.isDone();
    }
    
    @Override
    public Object get()
        throws InterruptedException, ExecutionException
    {
        sync.acquire(-1);
        if (this.response != null)
        {
            return this.response.getResult();
        }
        else
        {
            return null;
        }
    }
    
    @Override
    public Object get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
        boolean success = sync.tryAcquireNanos(-1, unit.toNanos(timeout));
        if (success)
        {
            if (this.response != null)
            {
                insertAccesslog();
                return this.response.getResult();
            }
            else
            {
                return null;
            }
        }
        else
        {
            this.response.setError("Timeout Exception");
            insertAccesslog();
            
            throw new RuntimeException(
                "Timeout exception. Request id: " + this.request.getRequestId() + ". Request class name: "
                    + this.request.getClassName() + ". Request method: " + this.request.getMethodName());
        }
    }
    
    @Override
    public boolean isCancelled()
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        throw new UnsupportedOperationException();
    }
    
    public void done(RpcResponse reponse)
    {
        this.response = reponse;
        
        // Threshold
        long responseTime = System.currentTimeMillis() - startTime;
        if (responseTime > this.responseTimeThreshold)
        {
            LOGGER.warn("Service response time is too slow. Request id = " + reponse.getRequestId()
                + ". Response Time = " + responseTime + "ms");
        }
        
        sync.release(1);
        invokeCallbacks();
    }
    
    private void insertAccesslog()
    {
        long responseTime = System.currentTimeMillis() - startTime;
        
        String localAddress = CommonUtil.getServiceAddress(localAddr);
        String remoteAddress = CommonUtil.getServiceAddress(remoteAddr);
        String requestJson = CommonUtil.convertObj2Json(this.request);
        String responseJson = CommonUtil.convertObj2Json(this.response);
        
        AccessLog accessLog = new AccessLog();
        accessLog.setRequestId(this.request.getRequestId());
        accessLog.setClientAddress(localAddress);
        accessLog.setServerAddress(remoteAddress);
        accessLog.setReqJson(requestJson);
        accessLog.setRespJson(responseJson);
        accessLog.setCostTimeMills(responseTime);
        
        try
        {
            AccessLogService accessLogService = CrossClientInitializer.APPLICATION_CONTEXT.getBean(AccessLogService.class);
            accessLogService.insert(accessLog);
        }
        catch (Exception e)
        {
            LOGGER.debug(e.getMessage());
        }
    }
    
    private void invokeCallbacks()
    {
        lock.lock();
        try
        {
            for (final AsyncRPCCallback callback : pendingCallbacks)
            {
                runCallback(callback);
            }
        }
        finally
        {
            lock.unlock();
        }
    }
    
    public RPCFuture addCallback(AsyncRPCCallback callback)
    {
        lock.lock();
        try
        {
            if (isDone())
            {
                runCallback(callback);
            }
            else
            {
                this.pendingCallbacks.add(callback);
            }
        }
        finally
        {
            lock.unlock();
        }
        return this;
    }
    
    private void runCallback(final AsyncRPCCallback callback)
    {
        final RpcResponse res = this.response;
        threadPoolExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                if (!res.isError())
                {
                    callback.success(res.getResult());
                }
                else
                {
                    callback.fail(new RuntimeException("Response error", new Throwable(res.getError())));
                }
            }
        });
    }
    
    static class Sync extends AbstractQueuedSynchronizer
    {
        
        private static final long serialVersionUID = 1L;
        
        // future status
        private final int done = 1;
        
        private final int pending = 0;
        
        protected boolean tryAcquire(int acquires)
        {
            return getState() == done ? true : false;
        }
        
        protected boolean tryRelease(int releases)
        {
            if (getState() == pending)
            {
                if (compareAndSetState(pending, done))
                {
                    return true;
                }
            }
            return false;
        }
        
        public boolean isDone()
        {
            getState();
            return getState() == done;
        }
    }
}
