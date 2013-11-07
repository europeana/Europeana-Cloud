package eu.europeana.cloud.service.mcs.exception;

/**
 * DataSetNotExistsException
 */
public class DataSetNotExistsException extends RuntimeException {

    public DataSetNotExistsException() {
    }


    public DataSetNotExistsException(String message) {
        super(message);
    }
}