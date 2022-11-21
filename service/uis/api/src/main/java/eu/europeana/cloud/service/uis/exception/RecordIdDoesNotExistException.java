package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Exception thrown when the record id does not exist
 *
 * @deprecated
 */
@Deprecated(since = "7-SNAPSHOT")
public class RecordIdDoesNotExistException extends GenericException {

  /**
   *
   */
  private static final long serialVersionUID = 1788394853781008988L;

  /**
   * Creates a new instance of this class.
   *
   * @param errorInfo Error info
   */
  public RecordIdDoesNotExistException(ErrorInfo errorInfo) {
    super(errorInfo);
  }

  /**
   * Creates a new instance of this class.
   *
   * @param errorInfo Error info
   */
  public RecordIdDoesNotExistException(IdentifierErrorInfo errorInfo) {
    super(errorInfo);
  }


}
