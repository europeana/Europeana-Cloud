package eu.europeana.cloud.service.web.common;

public class LoggingMessageBuilder {

  private static final String LOG_MESSAGE_PATTERN = "%STATUS_CODE %REQUEST_TIME %CLIENT_IP %CLIENT_USERNAME %REQUEST_METHOD %RESOURCE_PATH Request received";
  private int statusCode;
  private long requestTime;
  private String remoteAddr;
  private String resourcePath;
  private String requestMethod;
  private String clientUsername;


  public LoggingMessageBuilder() {
  }

  public LoggingMessage build() {
    String message = LOG_MESSAGE_PATTERN;
    message = message.replace("%STATUS_CODE", statusCode + "");
    message = message.replace("%REQUEST_TIME", requestTime + "ms");
    message = message.replace("%CLIENT_IP", remoteAddr);
    message = message.replace("%RESOURCE_PATH", resourcePath);
    message = message.replace("%REQUEST_METHOD", requestMethod);
    message = message.replace("%CLIENT_USERNAME", clientUsername == null ? "" : clientUsername);

    LoggingMessage m = new LoggingMessage();
    m.setMessage(message);
    m.setStatus(statusCode);
    return m;
  }

  public LoggingMessageBuilder withStatusCode(int statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public LoggingMessageBuilder withRequestTime(long requestTime) {
    this.requestTime = requestTime;
    return this;
  }

  public LoggingMessageBuilder withClientUsername(String clientUsername) {
    this.clientUsername = clientUsername;
    return this;
  }

  public LoggingMessageBuilder withRemoteAddr(String remoteAddr) {
    this.remoteAddr = remoteAddr;
    return this;
  }

  public LoggingMessageBuilder withResourcePath(String resourcePath) {
    this.resourcePath = resourcePath;
    return this;
  }

  public LoggingMessageBuilder withRequestMethod(String requestMethod) {
    this.requestMethod = requestMethod;
    return this;
  }
}
