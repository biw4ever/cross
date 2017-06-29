package com.yjz.cross.codec;

/**
 * RPC Response
 * @author biw
 */
public class RpcResponse {

    private String requestId;
    private String error;
    private Object result;
    private Class<?> returnType;

    public boolean isError() {
        return error != null;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Class<?> getReturnType()
    {
        return returnType;
    }

    public void setReturnType(Class<?> returnType)
    {
        this.returnType = returnType;
    }
}
