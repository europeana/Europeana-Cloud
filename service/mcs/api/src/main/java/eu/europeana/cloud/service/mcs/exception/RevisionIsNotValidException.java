package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to sdd unvalidated revision as parameter.
 */
public class RevisionIsNotValidException extends MCSException {

  /**
   * Constructs a RevisionIsNotValidException with no specified detail message.
   */
  public RevisionIsNotValidException() {
  }


  /**
   * Constructs a RevisionIsNotValidException with the specified detail message.
   *
   * @param message the detail message
   */
  public RevisionIsNotValidException(String message) {
    super(message);
  }
}
