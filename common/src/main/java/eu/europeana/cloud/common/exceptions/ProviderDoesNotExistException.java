package eu.europeana.cloud.common.exceptions;

import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Exception thrown when the provider Id does not exist
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class ProviderDoesNotExistException extends GenericException {

  private static final long serialVersionUID = 468350985177621312L;

  /**
   * Creates a new instance of this class.
   *
   * @param e
   */
  public ProviderDoesNotExistException(ErrorInfo e) {
    super(e);
  }

  /**
   * Creates a new instance of this class.
   *
   * @param errorInfo
   */
  public ProviderDoesNotExistException(IdentifierErrorInfo errorInfo) {
    super(errorInfo);
  }
}
