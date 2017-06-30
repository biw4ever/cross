package com.yjz.cross.client.service.impl;

import org.springframework.stereotype.Service;

import com.yjz.cross.client.annotation.CrossReference;
import com.yjz.cross.client.service.AccessLogService;
import com.yjz.cross.monitor.pojo.AccessLog;

@Service
public class AccessLogServiceImpl implements AccessLogService
{
    @CrossReference
    private com.yjz.cross.monitor.service.AccessLogService accessLogService;
    
    @Override
    public int insert(AccessLog accessLog)
    {
        return accessLogService.insert(accessLog);
    }
    
}
