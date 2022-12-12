package eu.europeana.cloud.service.commons.utils;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.validateMockitoUsage;

import eu.europeana.cloud.common.annotation.Retryable;
import java.time.Instant;
import java.util.Optional;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@ContextConfiguration(classes = {RetryAspectConfiguration.class})
@RunWith(SpringRunner.class)
public class RetryAspectSpringTest {

  @Autowired
  private AspectedTestSpringCtx aspectedTest;


  @After
  public void validate() {
    validateMockitoUsage();
  }

  @Before
  public void resetData() {
    aspectedTest.resetAttempts();
  }

  @Test
  public void shouldCallDefault3Times() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    String result = aspectedTest.test_default("Text to process");
    long endTime = Instant.now().toEpochMilli();

    assertTrue(result.contains("Text to process"));
    assertTrue(endTime - startTime >= 2 * 1000);
  }

  @Test
  public void shouldCall10Times() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_500_10();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 9 * 500);
  }

  @Test
  public void shouldCall6TimesAndFail() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    assertThrows(TestRuntimeExpection.class, () -> aspectedTest.test_delay_2000_6()

    );
    long endTime = Instant.now().toEpochMilli();
    assertTrue(endTime - startTime >= 5 * 2000);
  }

  @Test
  public void shouldCall4Times() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_3000_4();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 3 * 3000);
  }

  @Test
  public void shouldOverrideRetryParams() {
    Assume.assumeTrue(RetryableMethodExecutor.areRetryParamsOverridden());
    int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(Retryable.DEFAULT_MAX_ATTEMPTS);
    int delay = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_DELAY_BETWEEN_ATTEMPTS)
                        .orElse(Retryable.DEFAULT_DELAY_BETWEEN_ATTEMPTS);

    long startTime = Instant.now().toEpochMilli();
    aspectedTest.failGivenAmountOfTimes(attemptCount - 1, delay);
    long endTime = Instant.now().toEpochMilli();

    aspectedTest.resetAttempts();
    assertTrue(endTime - startTime >= (long) (attemptCount - 1) * delay);
    assertThrows(TestRuntimeExpection.class, () -> aspectedTest.failGivenAmountOfTimes(attemptCount, delay));
  }
}
