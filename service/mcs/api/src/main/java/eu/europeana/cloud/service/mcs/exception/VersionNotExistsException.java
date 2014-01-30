package eu.europeana.cloud.service.mcs.exception;

/**
 * Use {@link RepresentationNotExistsException} instead of this exception.
 */
@Deprecated
public class VersionNotExistsException extends RepresentationNotExistsException {

    /**
     * Constructs a VersionNotExistsException with no specified detail message.
     */
    public VersionNotExistsException() {
    }


    /**
     * Constructs a VersionNotExistsException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public VersionNotExistsException(String message) {
        super(message);
    }
}
