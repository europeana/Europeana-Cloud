package eu.europeana.cloud.service.mcs.exception;

/**
 * RecordNotExistsException
 */
public class VersionNotExistsException extends RepresentationNotExistsException {

    public VersionNotExistsException() {
    }


    public VersionNotExistsException(String message) {
        super(message);
    }
}
