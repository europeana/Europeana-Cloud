package eu.europeana.cloud.service.commons.utils;

public class RetryableProxyCreateException extends RuntimeException {

  public RetryableProxyCreateException(ReflectiveOperationException e) {
    super("Could not create retry proxy!", e);
  }
}
