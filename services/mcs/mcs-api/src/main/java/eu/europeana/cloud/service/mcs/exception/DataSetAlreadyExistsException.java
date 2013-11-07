package eu.europeana.cloud.service.mcs.exception;

/**
 * DataSetAlreadyExistsException
 */
public class DataSetAlreadyExistsException extends RuntimeException {

    public DataSetAlreadyExistsException(String message) {
        super(message);
    }


    public DataSetAlreadyExistsException() {
    }
}