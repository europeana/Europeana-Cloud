package eu.europeana.cloud.service.commons.utils;

import eu.europeana.cloud.common.annotation.Retryable;

public interface AspectedTest1Interface {
    String testMethod01_fails_2(String s1, int i2);
    void testMethod02_fails_4(Object p1, Object p2);
    String testMethod03_fails_1();
    String testMethod04_fails_3();
}
