package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there was attempt to create a data set which already exists for specific data provider.
 */
public class DataSetAlreadyExistsException extends MCSException {

    /**
     * Constructs a DataSetAlreadyExistsException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public DataSetAlreadyExistsException(String message) {
        super(message);
    }


    /**
     * Constructs a DataSetAlreadyExistsException with no specified detail message.
     */
    public DataSetAlreadyExistsException() {
    }
}
