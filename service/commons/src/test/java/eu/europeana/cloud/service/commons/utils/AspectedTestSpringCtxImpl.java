package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

@Retryable(delay = 100)
public class AspectedTestSpringCtxImpl implements AspectedTestSpringCtx {

  private int attemptNumber = 0;

  public void resetAttempts() {
    attemptNumber = 0;
  }

  public String test_default(String someData) {
    throwTwoTimesException(2);
    return String.format("Processed '%s'", someData);
  }

  public void test_delay_100_retries_10() {
    throwTwoTimesException(9);
  }

  public void test_delay_100_retries_6() {
    throwTwoTimesException(6);
  }

  public void test_delay_100_retries_4() {
    throwTwoTimesException(3);
  }


  private void throwTwoTimesException(int exceptionsNumber) {
    attemptNumber++;
    if (attemptNumber <= exceptionsNumber) {
      throw new TestRuntimeExpection();
    }
  }
}
