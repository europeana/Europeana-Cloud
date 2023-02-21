package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

public interface AspectedTestSpringCtx {

  void resetAttempts();

  String test_default(String someData);

  @Retryable
  void failGivenAmountOfTimes(int failCount, int delay);

  @Retryable(delay = 500, maxAttempts = 10)
  void test_delay_500_10();

  @Retryable(delay = 2000, maxAttempts = 6)
  void test_delay_2000_6();

  @Retryable(delay = 3000, maxAttempts = 4)
  void test_delay_3000_4();

}
