package eu.europeana.cloud.service.commons.utils;

import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RetryAspectTest {

  private AspectedTest1Impl aspectTestTarget;
  private AspectedTest1Interface aspectTestProxy;

  @Before
  public void prepareTests() {
    aspectTestTarget = spy(new AspectedTest1Impl());

    AspectJProxyFactory factory = new AspectJProxyFactory(aspectTestTarget);
    RetryAspect aspect = new RetryAspect();
    factory.addAspect(aspect);

    aspectTestProxy = factory.getProxy();
  }

  @Test
  public void shouldCallThreeTimesAspectedMethodWithSuccess() {
    String result = aspectTestProxy.testMethod01_fails_2("test 01", 1);
    verify(aspectTestTarget, times(3)).testMethod01_fails_2("test 01", 1);

    assertTrue(result.startsWith("test 01"));
    assertTrue(result.endsWith("1"));
  }

  @Test(expected = TestRuntimeExpection.class)
  public void shouldCallThreeTimesAspectedMethodWithoutSuccess() {
    aspectTestProxy.testMethod02_fails_4("s1", "s2");
    verify(aspectTestTarget, times(3)).testMethod02_fails_4("s1", "s2");
  }

  @Test
  public void shouldCallTwoTimesOneFailLongDelayAspectedMethodWithSuccess() {
    String result = aspectTestProxy.testMethod03_fails_1();
    verify(aspectTestTarget, times(2)).testMethod03_fails_1();
    assertThat("Success call. Non null result", result, equalTo("SUCCESS"));
  }

  @Test(expected = TestRuntimeExpection.class)
  public void shouldCallTwoTimesThreeFailAspectedMethodWithoutSuccess() {
    String result = aspectTestProxy.testMethod04_fails_3();
    verify(aspectTestTarget, times(2)).testMethod04_fails_3();
    assertNull("Expected that call fails. Null result", result);
  }

  @Test(expected = TestRuntimeExpection.class)
  public void shouldCallThreeTimesOnlyForAnnotatedMethodWithoutSuccess() {
    //prepare test
    AspectedTest2Impl aspectTestTarget = spy(new AspectedTest2Impl());

    AspectJProxyFactory factory = new AspectJProxyFactory(aspectTestTarget);
    RetryAspect aspect = new RetryAspect();
    factory.addAspect(aspect);

    AspectedTest2Interface aspectTestProxy = factory.getProxy();

    //do test
    aspectTestProxy.testMethod05_fails_3(anyString(), anyInt());
    verify(aspectTestTarget, times(3)).testMethod05_fails_3(anyString(), anyInt());
  }


}
