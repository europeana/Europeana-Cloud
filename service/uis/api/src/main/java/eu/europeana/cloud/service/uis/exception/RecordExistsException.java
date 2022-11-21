package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Exception thrown when a record already exists in the database
 *
 * @author Yorgos.Mamakis@ kb.nl
 */
public class RecordExistsException extends GenericException {

  /**
   *
   */
  private static final long serialVersionUID = 3302765474657090505L;

  /**
   * Creates a new instance of this class.
   *
   * @param errorInfo
   */
  public RecordExistsException(ErrorInfo errorInfo) {
    super(errorInfo);
  }


  /**
   * Creates a new instance of this class.
   *
   * @param errorInfo
   */
  public RecordExistsException(IdentifierErrorInfo errorInfo) {
    super(errorInfo);
  }


}
