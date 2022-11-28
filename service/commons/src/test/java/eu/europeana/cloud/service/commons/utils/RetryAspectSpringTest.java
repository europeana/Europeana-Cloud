package eu.europeana.cloud.service.commons.utils;

import static org.junit.Assert.assertTrue;

import java.time.Instant;
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

  @Before
  public void resetData() {
    aspectedTest.resetAttempts();
  }

  @Test
  public void shoudCallDefault3Times() {
    long startTime = Instant.now().toEpochMilli();
    String result = aspectedTest.test_default("Text to process");
    long endTime = Instant.now().toEpochMilli();

    assertTrue(result.contains("Text to process"));
    assertTrue(endTime - startTime >= 2 * 1000);
  }

  @Test
  public void shoudCall10Times() {
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_500_10();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 9 * 500);
  }

  @Test(expected = TestRuntimeExpection.class)
  public void shoudCall6TimesAndFail() {
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_2000_6();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 5 * 2000);
  }

  @Test
  public void shoudCall4Times() {
    long startTime = Instant.now().toEpochMilli();
    aspectedTest.test_delay_3000_4();
    long endTime = Instant.now().toEpochMilli();

    assertTrue(endTime - startTime >= 3 * 3000);
  }
}
