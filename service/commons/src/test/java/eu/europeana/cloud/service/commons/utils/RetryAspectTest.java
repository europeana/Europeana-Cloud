package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.*;

/**
 * Several tests from this class is skipped when overridden value for retries attempt is set because it makes those tests
 * purposeless. In case when that value is set then some extra tests will be executed in order to assure that overriding work as
 * intended
 */
class RetryAspectTest {

  private static AspectedTest1Impl aspectTestTarget;
  private static AspectedTest1Interface aspectTestProxy;

  @BeforeEach
  void prepareTests() {
    aspectTestTarget = spy(new AspectedTest1Impl());

    AspectJProxyFactory factory = new AspectJProxyFactory(aspectTestTarget);
    RetryAspect aspect = new RetryAspect();
    factory.addAspect(aspect);

    aspectTestProxy = factory.getProxy();
  }


  @Test
  void shouldCallThreeTimesAspectedMethodWithSuccess() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    String result = aspectTestProxy.testMethod01_fails_2("test 01", 1);
    verify(aspectTestTarget, times(3)).testMethod01_fails_2("test 01", 1);

    assertTrue(result.startsWith("test 01"));
    assertTrue(result.endsWith("1"));
  }

  @Test
  void shouldCallThreeTimesAspectedMethodWithoutSuccess() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    assertThrows(TestRuntimeExpection.class, () -> aspectTestProxy.testMethod02_fails_4("s1", "s2"));
    verify(aspectTestTarget, times(3)).testMethod02_fails_4("s1", "s2");
  }

  @Test
  void shouldCallTwoTimesOneFailLongDelayAspectedMethodWithSuccess() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    String result = aspectTestProxy.testMethod03_fails_1();
    verify(aspectTestTarget, times(2)).testMethod03_fails_1();
    assertEquals("SUCCESS", result);
  }

  @Test
  void shouldCallTwoTimesThreeFailAspectedMethodWithoutSuccess() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    assertThrows(TestRuntimeExpection.class, () -> aspectTestProxy.testMethod04_fails_3());
    verify(aspectTestTarget, times(2)).testMethod04_fails_3();
  }

  @Test
  void shouldCallThreeTimesOnlyForAnnotatedMethodWithoutSuccess() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
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
  void shouldOverrideRetryParamsAndMethodShouldSuccessAfterFailingAllowedNumberOfTimes() {
    assumeTrue(RetryableMethodExecutor.areRetryParamsOverridden());
    int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(
            Retryable.DEFAULT_MAX_ATTEMPTS);
    aspectTestProxy.failGivenAmountOfTimes(attemptCount - 1);
    verify(aspectTestTarget, times(attemptCount)).failGivenAmountOfTimes(anyInt());
  }

  @Test
  void shouldOverrideRetryParamsAndThrowExceptionAfterFailingAllRetries() {
    assumeTrue(RetryableMethodExecutor.areRetryParamsOverridden());
    int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(
            Retryable.DEFAULT_MAX_ATTEMPTS);
    assertThrows(TestRuntimeExpection.class, () -> aspectTestProxy.failGivenAmountOfTimes(attemptCount));
    verify(aspectTestTarget, times(attemptCount)).failGivenAmountOfTimes(anyInt());
  }

}
