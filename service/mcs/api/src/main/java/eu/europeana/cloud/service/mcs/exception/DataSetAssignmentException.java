package eu.europeana.cloud.service.mcs.exception;

/**
 * Exception related with incorrect assignments of the representation versions to the datasets
 */
public class DataSetAssignmentException extends MCSException {

  public DataSetAssignmentException(String message) {
    super(message);
  }
}
