package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.validateMockitoUsage;

/**
 * Several tests from this class is skipped when overridden value for retries attempt is set because it makes those tests
 * purposeless. In case when that value is set then some extra tests will be executed in order to assure that overriding work as
 * intended
 */
@ContextConfiguration(classes = {RetryAspectConfiguration.class})
@ExtendWith(SpringExtension.class)
public class RetryAspectSpringTest {

  @Autowired
  private AspectedTestSpringCtx aspectedTest;


  @AfterEach
  public void validate() {
    validateMockitoUsage();
  }

  @BeforeEach
  public void resetData() {
    aspectedTest.resetAttempts();
  }

  @Test
  public void shouldCallDefault3Times() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    String result = aspectedTest.test_default("Text to process");
    long endTime = Instant.now().toEpochMilli();

    assertTrue(result.contains("Text to process"));
    assertTrue(endTime - startTime >= 2 * 1000);
  }

  @Test
  public void shouldCall10Times() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_500_10();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 9 * 500);
  }

  @Test
  public void shouldCall6TimesAndFail() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    assertThrows(TestRuntimeExpection.class, () -> aspectedTest.test_delay_2000_6());
    long endTime = Instant.now().toEpochMilli();
    assertTrue(endTime - startTime >= 5 * 2000);
  }

  @Test
  public void shouldCall4Times() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_3000_4();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 3 * 3000);
  }

  @Test
  public void shouldOverrideRetryParamsAndMethodShouldSuccessAfterFailingAllowedNumberOfTimes() {
    assumeTrue(RetryableMethodExecutor.areRetryParamsOverridden());
    int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(Retryable.DEFAULT_MAX_ATTEMPTS);
    int delay = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_DELAY_BETWEEN_ATTEMPTS)
                        .orElse(Retryable.DEFAULT_DELAY_BETWEEN_ATTEMPTS);

    long startTime = Instant.now().toEpochMilli();
    aspectedTest.failGivenAmountOfTimes(attemptCount - 1, delay);
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= (long) (attemptCount - 1) * delay);
  }

  @Test
  public void shouldOverrideRetryParamsAndThrowExceptionAfterFailingAllRetries() {
    assumeTrue(RetryableMethodExecutor.areRetryParamsOverridden());
    int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(Retryable.DEFAULT_MAX_ATTEMPTS);
    int delay = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_DELAY_BETWEEN_ATTEMPTS)
                        .orElse(Retryable.DEFAULT_DELAY_BETWEEN_ATTEMPTS);
    assertThrows(TestRuntimeExpection.class, () -> aspectedTest.failGivenAmountOfTimes(attemptCount, delay));
  }

}
