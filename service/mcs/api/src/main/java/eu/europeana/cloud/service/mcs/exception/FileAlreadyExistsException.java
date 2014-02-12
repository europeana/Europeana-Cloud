package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown whene there is attempt to create a file which is already attached to a specific representation version.
 */
public class FileAlreadyExistsException extends BaseMCSException {

    /**
     * Constructs a FileAlreadyExistsException with no specified detail message.
     */
    public FileAlreadyExistsException() {
    }


    /**
     * Constructs a FileAlreadyExistsException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public FileAlreadyExistsException(String message) {
        super(message);
    }
}
