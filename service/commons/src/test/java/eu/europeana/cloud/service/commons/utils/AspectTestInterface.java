package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

public interface AspectTestInterface {
    @Retryable(delay = 1000)
    String testMethod01(String s1, int i2);

    @Retryable(delay = 1000)
    void testMethod02(Object p1, Object p2);

    @Retryable(maxAttempts = 2, delay = 20*1000)
    String testMethod03();

    @Retryable(maxAttempts = 2, delay = 1000)
    String testMethod04();
}
