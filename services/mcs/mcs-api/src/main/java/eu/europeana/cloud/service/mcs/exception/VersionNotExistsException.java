package eu.europeana.cloud.service.mcs.exception;

/**
 * Use {@link RepresentationNotExistsException} instead of this exception.
 */
@Deprecated
public class VersionNotExistsException extends RepresentationNotExistsException {

    public VersionNotExistsException() {
    }


    public VersionNotExistsException(String message) {
        super(message);
    }
}
