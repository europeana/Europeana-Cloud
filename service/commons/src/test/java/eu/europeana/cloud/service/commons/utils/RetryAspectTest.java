package eu.europeana.cloud.service.commons.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import eu.europeana.cloud.common.annotation.Retryable;
import java.util.Optional;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

public class RetryAspectTest {

  private static AspectedTest1Impl aspectTestTarget;
  private static AspectedTest1Interface aspectTestProxy;

  @BeforeClass
  public static void prepareTests() {
    aspectTestTarget = spy(new AspectedTest1Impl());

    AspectJProxyFactory factory = new AspectJProxyFactory(aspectTestTarget);
    RetryAspect aspect = new RetryAspect();
    factory.addAspect(aspect);

    aspectTestProxy = factory.getProxy();
  }

  @Before
  public void resetAttempts() {
    aspectTestTarget.resetAttempts();
  }

  @Test
  public void shouldCallThreeTimesAspectedMethodWithSuccess() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    String result = aspectTestProxy.testMethod01_fails_2("test 01", 1);
    verify(aspectTestTarget, times(3)).testMethod01_fails_2("test 01", 1);

    assertTrue(result.startsWith("test 01"));
    assertTrue(result.endsWith("1"));
  }

  @Test
  public void shouldCallThreeTimesAspectedMethodWithoutSuccess() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    assertThrows(TestRuntimeExpection.class, () -> aspectTestProxy.testMethod02_fails_4("s1", "s2"));
    verify(aspectTestTarget, times(3)).testMethod02_fails_4("s1", "s2");
  }

  @Test
  public void shouldCallTwoTimesOneFailLongDelayAspectedMethodWithSuccess() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    String result = aspectTestProxy.testMethod03_fails_1();
    verify(aspectTestTarget, times(2)).testMethod03_fails_1();
    assertEquals("SUCCESS", result);
  }

  @Test
  public void shouldCallTwoTimesThreeFailAspectedMethodWithoutSuccess() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    assertThrows(TestRuntimeExpection.class, () -> aspectTestProxy.testMethod04_fails_3());
    verify(aspectTestTarget, times(2)).testMethod04_fails_3();
  }

  @Test
  public void shouldCallThreeTimesOnlyForAnnotatedMethodWithoutSuccess() {
    Assume.assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    //prepare test
    AspectedTest2Impl aspectTestTarget = spy(new AspectedTest2Impl());

    AspectJProxyFactory factory = new AspectJProxyFactory(aspectTestTarget);
    RetryAspect aspect = new RetryAspect();
    factory.addAspect(aspect);

    AspectedTest2Interface aspectTestProxy = factory.getProxy();

    //do test
    assertThrows(TestRuntimeExpection.class, () -> aspectTestProxy.testMethod05_fails_3("test", 1));
    verify(aspectTestTarget, times(3)).testMethod05_fails_3(anyString(), anyInt());
  }

  @Test
  public void testIfOverriddenRetryParams() {
    Assume.assumeTrue(RetryableMethodExecutor.areRetryParamsOverridden());
    int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(
        Retryable.DEFAULT_MAX_ATTEMPTS);
    aspectTestProxy.failGivenAmountOfTimes(attemptCount - 1);
    verify(aspectTestTarget, times(attemptCount)).failGivenAmountOfTimes(anyInt());
    aspectTestProxy.resetAttempts();
    assertThrows(TestRuntimeExpection.class, () -> aspectTestProxy.failGivenAmountOfTimes(attemptCount));
  }

}
