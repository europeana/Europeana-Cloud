package eu.europeana.cloud.service.commons.logging;

import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.*;
import javax.ws.rs.core.Context;
import java.io.IOException;

@PreMatching
public class LoggingFilter implements ContainerResponseFilter, ContainerRequestFilter {

    @Context
    HttpServletRequest httpRequest;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LoggingFilter.class);

    
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        publishRequestStartTimeTo(requestContext);
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        LoggingMessage message = buildMessage(requestContext, responseContext);
        switch (message.getStatus()){
            case 404:
            case 409:
                LOGGER.warn(message.getMessage());
                break;
            default:
                LOGGER.info(message.getMessage());
                break;
        }
    }

    private void publishRequestStartTimeTo(ContainerRequestContext requestContext) {
        long requestStartTime = System.currentTimeMillis();
        requestContext.setProperty("startTime", requestStartTime);
    }

    private LoggingMessage buildMessage(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        //
        LoggingMessageBuilder builder = new LoggingMessageBuilder();
        
        LoggingMessage message = builder
                .withStatusCode(responseContext.getStatus())
                .withRemoteAddr(readRemoteAddr())
                .withRequestTime(calculateRequestTime(requestContext))
                .withResourcePath(readRequestPath(requestContext))
                .withRequestMethod(readRequestMethod(requestContext))
                .build();
        
        return message;
    }
    
    private String readRemoteAddr(){
        try{
            return httpRequest.getRemoteAddr();
        }catch (Exception e){
            return "error.remote.address";
        }
    }
    
    private long calculateRequestTime(ContainerRequestContext requestContext){
        long now = System.currentTimeMillis();
        long requestTime = now - (long) requestContext.getProperty("startTime");
        return requestTime;
    }
    
    private String readRequestPath(ContainerRequestContext requestContext){
        return requestContext.getUriInfo().getPath().replaceAll(" ","%20");
    }
    
    private String readRequestMethod(ContainerRequestContext requestContext){
        return requestContext.getMethod();
    }
}
