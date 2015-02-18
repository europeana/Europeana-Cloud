package eu.europeana.cloud.service.commons.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerFactory;
import sun.security.provider.certpath.OCSPResponse;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

@PreMatching
public class LoggingFilter implements ContainerResponseFilter, ContainerRequestFilter {

    @Context
    HttpServletRequest httpRequest;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LoggingFilter.class);

    private StringBuilder logMessageBuilder;

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
                .withResourcePatch(readRequestPath(requestContext))
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
}
