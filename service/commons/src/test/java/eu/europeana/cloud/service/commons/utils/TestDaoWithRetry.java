package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

public class TestDaoWithRetry {

  @Retryable(delay = 100, maxAttempts = 3)
  public void retryableMethod() throws TestRuntimeExpection {
    throw new TestRuntimeExpection();
  }

  public void noRetryableMethod() throws TestRuntimeExpection {
    throw new TestRuntimeExpection();
  }

  @Retryable(delay = 100, maxAttempts = 3)
  public void noErrorMethod() throws TestRuntimeExpection {
  }


}
