package com.yjz.cross.monitor.pojo;

public class AccessLog
{
private String requestId;
    
    private String serverAddress;
    
    private String clientAddress;
    
    private String reqJson;
    
    private String respJson;
    
    private long costTimeMills;

    public String getRequestId()
    {
        return requestId;
    }

    public void setRequestId(String requestId)
    {
        this.requestId = requestId;
    }

    public String getServerAddress()
    {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress)
    {
        this.serverAddress = serverAddress;
    }

    public String getClientAddress()
    {
        return clientAddress;
    }

    public void setClientAddress(String clientAddress)
    {
        this.clientAddress = clientAddress;
    }

    public String getReqJson()
    {
        return reqJson;
    }

    public void setReqJson(String reqJson)
    {
        this.reqJson = reqJson;
    }

    public String getRespJson()
    {
        return respJson;
    }

    public void setRespJson(String respJson)
    {
        this.respJson = respJson;
    }

    public long getCostTimeMills()
    {
        return costTimeMills;
    }

    public void setCostTimeMills(long costTimeMills)
    {
        this.costTimeMills = costTimeMills;
    } 
}
