package eu.europeana.cloud.service.commons.logging;

public class LoggingMessageBuilder {

    private StringBuilder logMessageBuilder;
    
    private int statusCode;
    private long requestTime;
    private String remoteAddr;
    private String resourcePatch;
    
    private static String LOG_MESSAGE_PATTERN = "%STATUS_CODE %REQUEST_TIME %CLIENT_IP %RESOURCE_PATH Request received";
    
    public LoggingMessageBuilder(){
        logMessageBuilder = new StringBuilder();
    }
    
    public LoggingMessage build(){
        String message = LOG_MESSAGE_PATTERN;
        message = message.replace("%STATUS_CODE", statusCode+"");
        message = message.replace("%REQUEST_TIME",requestTime+"ms");
        message = message.replace("%CLIENT_IP", remoteAddr);
        message = message.replace("%RESOURCE_PATH", resourcePatch);
        
        LoggingMessage m = new LoggingMessage();
        m.setMessage(message);
        m.setStatus(statusCode);
        return m;
    }

    public LoggingMessageBuilder withStatusCode(int statusCode){
        this.statusCode = statusCode;
        return this;
    }

    public LoggingMessageBuilder withRequestTime(long requestTime){
        this.requestTime = requestTime;
        return this;
    }

    public LoggingMessageBuilder withRemoteAddr(String remoteAddr){
        this.remoteAddr = remoteAddr;
        return this;
    }

    public LoggingMessageBuilder withResourcePatch(String resourcePatch){
        this.resourcePatch = resourcePatch;
        return this;
    }
}