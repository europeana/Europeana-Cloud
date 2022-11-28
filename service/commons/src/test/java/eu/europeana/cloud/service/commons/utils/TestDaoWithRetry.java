package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.test.TestUtils;

public class TestDaoWithRetry {

  @Retryable(delay = TestUtils.DEFAULT_DELAY_BETWEEN_ATTEMPTS, maxAttempts = 3)
  public void retryableMethod() throws TestRuntimeExpection {
    throw new TestRuntimeExpection();
  }

  public void noRetryableMethod() throws TestRuntimeExpection {
    throw new TestRuntimeExpection();
  }

  @Retryable(delay = TestUtils.DEFAULT_DELAY_BETWEEN_ATTEMPTS, maxAttempts = 3)
  public void noErrorMethod() throws TestRuntimeExpection {
  }


}
