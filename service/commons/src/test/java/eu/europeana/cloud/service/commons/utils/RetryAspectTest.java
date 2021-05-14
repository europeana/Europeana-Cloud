package eu.europeana.cloud.service.commons.utils;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import static org.hamcrest.Matchers.*;


import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class RetryAspectTest {

    private AspectTestInterface aspectTestTarget;
    private AspectTestInterface aspectTestProxy;

    @Before
    public void prepareTests() {
        Map<String, Integer> failAttemptsCounter = new HashMap<>();
        failAttemptsCounter.put("testMethod01", 2);  // testMethod01 should have two (2) failed attempts
        failAttemptsCounter.put("testMethod02", 4);  // testMethod02 should have four (4) failed attempts
        failAttemptsCounter.put("testMethod03", 1);  // testMethod03 should have one (1) failed attempts
        //failAttemptsCounter.put("testMethod04", 3);  // testMethod04 should have three (3) failed attempts - THIS IS DEFAULT


        aspectTestTarget = spy(new AspectTestImpl(failAttemptsCounter));

        AspectJProxyFactory factory = new AspectJProxyFactory(aspectTestTarget);
        RetryAspect aspect = new RetryAspect();
        factory.addAspect(aspect);

        aspectTestProxy = factory.getProxy();
    }

    @Test
    public void shouldCallThreeTimesAspectedMethodWithSuccess() {
        String result = aspectTestProxy.testMethod01("test 01", 1);
        verify(aspectTestTarget, times(3)).testMethod01("test 01", 1);

        assertTrue(result.startsWith("test 01"));
        assertTrue(result.endsWith("1"));
    }

    @Test
    public void shouldCallThreeTimesAspectedMethodWithoutSuccess() {
        aspectTestProxy.testMethod02("s1", "s2");
        verify(aspectTestTarget, times(3)).testMethod02("s1", "s2");
    }

    @Test
    public void shouldCallTwoTimesOneFailLongDelayAspectedMethodWithSuccess() {
        String result = aspectTestProxy.testMethod03();
        verify(aspectTestTarget, times(2)).testMethod03();
        assertThat("Success call. Non null result", result, equalTo("SUCCESS"));
    }

    @Test
    public void shouldCallTwoTimesThreeFailAspectedMethodWithoutSuccess() {
        String result = aspectTestProxy.testMethod04();
        verify(aspectTestTarget, times(2)).testMethod04();
        assertNull("Expected that call fails. Null result", result);
    }

}
