package eu.europeana.cloud.test;

/**
 * Exception thrown when changing value of field via reflection didn't finish successfully due to either not found field name or
 * illegality of operation.
 */
public class CannotChangeFieldValueException extends RuntimeException {

  public CannotChangeFieldValueException(Throwable cause) {
    super("Cannot change field value!", cause);
  }
}
