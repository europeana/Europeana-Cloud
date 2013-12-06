package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown if there was attempt to use representation that does not exist (or representation in version that does not
 * exist).
 */
public class RepresentationNotExistsException extends RuntimeException {

    public RepresentationNotExistsException() {
    }


    public RepresentationNotExistsException(String message) {
        super(message);
    }
}
