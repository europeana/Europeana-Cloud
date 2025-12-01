package eu.europeana.cloud.service.dps.storm;

/**
 * The type Bolt finalization exception.
 */
public class BoltFinalizationException extends RuntimeException{

  /**
   * Instantiates a new Bolt finalization exception.
   *
   * @param message the message
   * @param cause the cause
   */
  public BoltFinalizationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Instantiates a new Bolt finalization exception.
   *
   * @param cause the cause
   */
  public BoltFinalizationException(Throwable cause) {
    super(cause);
  }
}
