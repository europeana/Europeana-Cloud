package eu.europeana.cloud.service.commons.utils;

public interface AspectedTest1Interface {

  String testMethod01_fails_2(String s1, int i2);

  void testMethod02_fails_4(Object p1, Object p2);

  String testMethod03_fails_1();

  void testMethod04_fails_3();

  void failGivenAmountOfTimes(int failCount);

  void resetAttempts();
}
