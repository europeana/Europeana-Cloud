package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get data set which does not exist.
 */
public class DataSetNotExistsException extends Exception {

    public DataSetNotExistsException() {
    }


    public DataSetNotExistsException(String message) {
        super(message);
    }
}
