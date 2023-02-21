package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

@Retryable
public class AspectedTestSpringCtxImpl implements AspectedTestSpringCtx {

  private int attemptNumber = 0;

  public void resetAttempts() {
    attemptNumber = 0;
  }

  public String test_default(String someData) {
    throwTwoTimesException(2);
    return String.format("Processed '%s'", someData);
  }

  @Override
  public void failGivenAmountOfTimes(int failCount, int delay) {
    throwTwoTimesException(failCount);
  }

  public void test_delay_500_10() {
    throwTwoTimesException(9);
  }

  public void test_delay_2000_6() {
    throwTwoTimesException(6);
  }

  public void test_delay_3000_4() {
    throwTwoTimesException(3);
  }


  private void throwTwoTimesException(int exceptionsNumber) {
    attemptNumber++;
    if (attemptNumber <= exceptionsNumber) {
      throw new TestRuntimeExpection();
    }
  }
}
