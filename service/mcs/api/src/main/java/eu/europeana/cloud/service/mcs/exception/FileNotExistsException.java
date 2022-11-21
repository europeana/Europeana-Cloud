package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get non existing file from a specific representation version.
 */
public class FileNotExistsException extends MCSException {

  /**
   * Constructs a FileNotExistsException with no specified detail message.
   */
  public FileNotExistsException() {
  }


  /**
   * Constructs a FileNotExistsException with the specified detail message.
   *
   * @param message the detail message
   */
  public FileNotExistsException(String message) {
    super(message);
  }
}
