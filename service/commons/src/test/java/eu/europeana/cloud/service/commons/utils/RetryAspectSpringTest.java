package eu.europeana.cloud.service.commons.utils;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

import java.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.AopTestUtils;

@ContextConfiguration(classes = {RetryAspectConfiguration.class})
@RunWith(SpringRunner.class)
public class RetryAspectSpringTest {


  @Autowired
  private AspectedTestSpringCtx aspectedTest;

  @Before
  public void resetData() {
    aspectedTest.resetAttempts();
  }

  @Test
  public void shouldCallDefault3Times() {
    long startTime = Instant.now().toEpochMilli();
    String result = aspectedTest.test_default("Text to process");
    long endTime = Instant.now().toEpochMilli();

    assertTrue(result.contains("Text to process"));
    assertTrue(endTime - startTime >= 2 * 100);
    Mockito.verify((AspectedTestSpringCtxImpl) AopTestUtils.getTargetObject(aspectedTest),
        Mockito.times(3)).test_default(anyString());
  }

  @Test
  public void shouldCall10Times() {
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_100_retries_10();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 9 * 100);
    Mockito.verify((AspectedTestSpringCtxImpl) AopTestUtils.getTargetObject(aspectedTest),
        Mockito.times(10)).test_delay_100_retries_10();
  }

  @Test
  public void shouldCall6TimesAndFail() {
    long startTime = Instant.now().toEpochMilli();
    try {
      aspectedTest.test_delay_100_retries_6();
    } catch (TestRuntimeExpection ignored) {
    }
    long endTime = Instant.now().toEpochMilli();
    assertTrue(endTime - startTime >= 5 * 100);
    Mockito.verify((AspectedTestSpringCtxImpl) AopTestUtils.getTargetObject(aspectedTest),
        Mockito.times(6)).test_delay_100_retries_6();
  }

  @Test
  public void shouldCall4Times() {
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_100_retries_4();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 2 * 100);
    Mockito.verify((AspectedTestSpringCtxImpl) AopTestUtils.getTargetObject(aspectedTest),
        Mockito.times(4)).test_delay_100_retries_4();
  }
}
