package eu.europeana.cloud.service.web.common;

public class LoggingMessage {

  private int status;
  private String message;


  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
