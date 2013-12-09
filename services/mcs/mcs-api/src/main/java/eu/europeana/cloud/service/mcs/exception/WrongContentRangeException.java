package eu.europeana.cloud.service.mcs.exception;

/**
 * Exception thrown when requesting for file content range that cannot be satisfied (falls beyond actual content bytes).
 */
public class WrongContentRangeException extends Exception {

    public WrongContentRangeException() {
    }


    public WrongContentRangeException(String message) {
        super(message);
    }
}
