package eu.europeana.cloud.service.dps.exception;

/**
 * Thrown when an internal error happened in Metadata and Content Service.
 */
public class DPSClientException extends RuntimeException {

  /**
   * Constructs a ServiceInternalErrorException with the specified detail message.
   *
   * @param message the detail message
   */
  public DPSClientException(String message) {
    super(message);
  }

  /**
   * Constructs a ServiceInternalErrorException with no specified detail message.
   */
  public DPSClientException() {
  }

  /**
   * Constructs a ServiceInternalErrorException with the specified detail message and inner Exception.
   *
   * @param message the detail message
   * @param e inner Exception
   */
  public DPSClientException(String message, Exception e) {
    super(message, e);
  }
}
