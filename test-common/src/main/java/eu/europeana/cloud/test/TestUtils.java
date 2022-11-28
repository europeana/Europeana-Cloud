package eu.europeana.cloud.test;

import java.lang.reflect.Field;

/**
 * Class for Util fields and methods for ecloud test classes
 */
public final class TestUtils {

  public static final int DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES = 2;
  public static final int DEFAULT_MAX_RETRY_COUNT = 0;
  public static final int DEFAULT_DELAY_BETWEEN_ATTEMPTS = 1;


  private TestUtils() {
  }

  public static void changeFieldValueForObject(Object objectToBeChanged, String fieldName, Object newValue) {
    try {
      Field field = objectToBeChanged.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(objectToBeChanged, newValue);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }


  public static void changeFieldValueForClass(Class objectClass, String fieldName, Object newValue) {
    try {
      Field field = objectClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(objectClass, newValue);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
