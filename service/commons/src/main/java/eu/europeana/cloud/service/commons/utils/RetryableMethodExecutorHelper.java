package eu.europeana.cloud.service.commons.utils;

public class RetryableMethodExecutorHelper {

  private RetryableMethodExecutorHelper() {
  }


  public static Integer getIntegerFromPropertyValue(String propertyValue) {
    return (propertyValue == null) ? null : Integer.parseInt(propertyValue);
  }

  public static Integer overrideWhenValueIsPresent(Integer baseValue, Integer overrideValue) {
    return (overrideValue == null) ? baseValue : overrideValue;
  }
}
