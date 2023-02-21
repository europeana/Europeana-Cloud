package eu.europeana.cloud.service.dps.exceptions;

/**
 * Runtime exception for CleanTaskDirService
 */
public class CleanTaskDirException extends RuntimeException {

  public CleanTaskDirException(String message) {
    super(message);
  }

  public CleanTaskDirException(String message, Throwable cause) {
    super(message, cause);
  }
}
