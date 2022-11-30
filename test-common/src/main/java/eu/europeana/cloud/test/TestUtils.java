package eu.europeana.cloud.test;

import java.lang.reflect.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class for Util fields and methods for ecloud test classes
 */
public final class TestUtils {

  private static final Log LOGGER = LogFactory.getLog(TestUtils.class);
  public static final int DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES = 2;
  public static final int DEFAULT_MAX_RETRY_COUNT = 0;
  public static final int DEFAULT_DELAY_BETWEEN_ATTEMPTS = 1;


  private TestUtils() {
  }

  /**
   * Change value of variable via reflection operation. Be aware that this function cannot modify final fields.
   *
   * @param objectToBeChanged object that field value will be changed
   * @param fieldName name of field that will be changed
   * @param newValue value that will be assigned to field
   */
  public static void changeFieldValueForObject(Object objectToBeChanged, String fieldName, Object newValue) {
    try {
      Field field = objectToBeChanged.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(objectToBeChanged, newValue);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOGGER.error("Exception occurred during reflection operation: ", e);
      throw new CannotChangeFieldValueException(e.getCause());
    }
  }

  /**
   * Change value of static variable of class via reflection operation. Be aware that this function cannot modify final fields.
   *
   * @param objectClass class that static field value will be changed
   * @param fieldName name of field that will be changed
   * @param newValue value that will be assigned to field
   */
  public static void changeFieldValueForClass(Class objectClass, String fieldName, Object newValue) {
    try {
      Field field = objectClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(objectClass, newValue);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOGGER.error("Exception occurred during reflection operation: ", e);
      throw new CannotChangeFieldValueException(e.getCause());
    }
  }
}
