package eu.europeana.cloud.service.web.common;

import static eu.europeana.cloud.common.log.AttributePassing.RECORD_ID_CONTEXT_ATTR;
import static eu.europeana.cloud.common.log.AttributePassing.TASK_ID_CONTEXT_ATTR;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.servlet.HandlerInterceptor;

public class LoggingFilter implements HandlerInterceptor {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LoggingFilter.class);
  private static final String REQUEST_ID_ATTRIBUTE_NAME = "requestId";

  @Override
  @SuppressWarnings("java:S2245") //Random is used here only for mark logs that are part of one request.
  //The usage is secure, because it is only for log readability.
  public boolean preHandle(HttpServletRequest servletRequest,
                           HttpServletResponse hsr1, Object handler) {

    MDC.put(REQUEST_ID_ATTRIBUTE_NAME, RandomStringUtils.randomAlphanumeric(6));
    readContextAttributes(servletRequest);
    publishRequestStartTimeTo(servletRequest);
    return true;
  }

  private void readContextAttributes(HttpServletRequest servletRequest) {
    String taskId = servletRequest.getHeader(TASK_ID_CONTEXT_ATTR);
    if(taskId != null) {
       MDC.put(TASK_ID_CONTEXT_ATTR,taskId);
    }

    String recordId = servletRequest.getHeader(RECORD_ID_CONTEXT_ATTR);
    if(recordId != null) {
      MDC.put(RECORD_ID_CONTEXT_ATTR,recordId);
    }
  }

  @Override
  public void afterCompletion(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
      Object handler, Exception exception) {
    LoggingMessage message = buildMessage(servletRequest, servletResponse);
    switch (message.getStatus()) {
      case 404:
      case 409:
        LOGGER.warn(message.getMessage());
        break;
      default:
        LOGGER.info(message.getMessage());
        break;
    }
    MDC.remove(REQUEST_ID_ATTRIBUTE_NAME);
    MDC.remove(TASK_ID_CONTEXT_ATTR);
    MDC.remove(RECORD_ID_CONTEXT_ATTR);
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
