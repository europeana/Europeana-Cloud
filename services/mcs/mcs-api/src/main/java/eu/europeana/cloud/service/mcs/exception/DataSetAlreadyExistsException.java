package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there was attempt to create a data set which already exists for specific data provider.
 */
public class DataSetAlreadyExistsException extends RuntimeException {

    public DataSetAlreadyExistsException(String message) {
        super(message);
    }


    public DataSetAlreadyExistsException() {
    }
}
