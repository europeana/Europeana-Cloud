package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get data set which does not exist.
 */
public class DataSetNotExistsException extends BaseMCSException {

    /**
     * Constructs a DataSetNotExistsException with no specified detail message.
     */
    public DataSetNotExistsException() {
    }


    /**
     * Constructs a DataSetNotExistsException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public DataSetNotExistsException(String message) {
        super(message);
    }
}
