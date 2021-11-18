package eu.europeana.cloud.service.mcs.utils;

import eu.europeana.cloud.service.commons.logging.LoggingMessage;
import eu.europeana.cloud.service.commons.logging.LoggingMessageBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LoggingFilter implements HandlerInterceptor {


    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingFilter.class);
    private static final String REQUEST_ID_ATTRIBUTE_NAME = "requestId";

    @Override
    public boolean preHandle(HttpServletRequest servletRequest,
                             HttpServletResponse servletResponse,
                             Object handler) {
        MDC.put(REQUEST_ID_ATTRIBUTE_NAME, RandomStringUtils.randomAlphanumeric(6));
        publishRequestStartTimeTo(servletRequest);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest servletRequest,
                                HttpServletResponse servletResponse,
                                Object handler, Exception exception) {

        LoggingMessage message = buildMessage(servletRequest, servletResponse);
        switch (message.getStatus()) {
            case HttpStatus.SC_NOT_FOUND:
            case HttpStatus.SC_CONFLICT:
                LOGGER.warn(message.getMessage());
                break;
            default:
                LOGGER.info(message.getMessage());
                break;
        }
        MDC.remove(REQUEST_ID_ATTRIBUTE_NAME);
    }

    private void publishRequestStartTimeTo(HttpServletRequest servletRequest) {
        long requestStartTime = System.currentTimeMillis();
        servletRequest.setAttribute("startTime", requestStartTime);
    }

    private LoggingMessage buildMessage(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        //
        LoggingMessageBuilder builder = new LoggingMessageBuilder();

        return builder
                .withStatusCode(servletResponse.getStatus())
                .withRemoteAddr(readRemoteAddr(servletRequest))
                .withRequestTime(calculateRequestTime(servletRequest))
                .withResourcePath(readRequestPath(servletRequest))
                .withRequestMethod(readRequestMethod(servletRequest))
                .build();
    }

    private String readRemoteAddr(HttpServletRequest servletRequest) {
        try {
            return servletRequest.getRemoteAddr();
        } catch (Exception e) {
            return "error.remote.address";
        }
    }

    private long calculateRequestTime(HttpServletRequest servletRequest) {
        long now = System.currentTimeMillis();
        return now - (long) servletRequest.getAttribute("startTime");
    }

    private String readRequestPath(HttpServletRequest servletRequest) {
        return servletRequest.getRequestURI();
    }

    private String readRequestMethod(HttpServletRequest servletRequest) {
        return servletRequest.getMethod();
    }
}
