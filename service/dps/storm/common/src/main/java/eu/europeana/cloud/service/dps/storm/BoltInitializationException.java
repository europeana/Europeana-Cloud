package eu.europeana.cloud.service.dps.storm;

public class BoltInitializationException extends RuntimeException{
  public BoltInitializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public BoltInitializationException(Throwable cause) {
    super(cause);
  }
}
