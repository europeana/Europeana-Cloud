package eu.europeana.cloud.service.mcs.exception;

/**
 * Exception thrown when requesting for file content range that cannot be satisfied (falls beyond actual content bytes).
 */
public class WrongContentRangeException extends Exception {

    /**
     * Constructs a WrongContentRangeException with no specified detail message.
     */
    public WrongContentRangeException() {
    }


    /**
     * Constructs a WrongContentRangeException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public WrongContentRangeException(String message) {
        super(message);
    }
}
