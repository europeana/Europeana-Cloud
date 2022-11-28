package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

public interface AspectedTestSpringCtx {

  void resetAttempts();

  String test_default(String someData);

  @Retryable(delay = 100, maxAttempts = 10)
  void test_delay_100_retries_10();

  @Retryable(delay = 100, maxAttempts = 6)
  void test_delay_100_retries_6();

  @Retryable(delay = 100, maxAttempts = 4)
  void test_delay_100_retries_4();

}
