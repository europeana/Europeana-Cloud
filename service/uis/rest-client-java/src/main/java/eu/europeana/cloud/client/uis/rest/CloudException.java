package eu.europeana.cloud.client.uis.rest;

/**
 * Generic Cloud Exception
 */
public class CloudException extends Exception {

  private static final long serialVersionUID = 8451384934113123019L;

  /**
   * Creates a new instance of this class with the wrapped cloud Exception
   *
   * @param message
   * @param cause The cloud exception to wrap
   */
  public CloudException(String message, Throwable cause) {
    super(message, cause);
  }
}
