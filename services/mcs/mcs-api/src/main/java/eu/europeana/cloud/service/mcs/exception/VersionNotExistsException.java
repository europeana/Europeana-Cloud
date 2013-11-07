package eu.europeana.cloud.service.mcs.exception;

/**
 * RecordNotExistsException
 */
public class VersionNotExistsException extends RuntimeException {

    public VersionNotExistsException() {
    }


    public VersionNotExistsException(String message) {
        super(message);
    }
}
