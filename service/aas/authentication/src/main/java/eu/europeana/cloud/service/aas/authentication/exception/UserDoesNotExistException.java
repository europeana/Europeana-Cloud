package eu.europeana.cloud.service.aas.authentication.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * User name not found exception
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 * @since 06.08.2014
 */
public class UserDoesNotExistException extends GenericException {

  private static final long serialVersionUID = 2743985314014225235L;

  /**
   * Creates a new instance of this class.
   *
   * @param e
   */
  public UserDoesNotExistException(ErrorInfo e) {
    super(e);
  }

  /**
   * Creates a new instance of this class.
   *
   * @param errorInfo
   */
  public UserDoesNotExistException(IdentifierErrorInfo errorInfo) {
    super(errorInfo);
  }
}
