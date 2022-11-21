package eu.europeana.cloud.service.commons.utils;

public class RetryInterruptedException extends RuntimeException {

  public RetryInterruptedException(Throwable e) {
    super("Stopped waiting for retry, because the thread was interrupted!", e);
  }
}
