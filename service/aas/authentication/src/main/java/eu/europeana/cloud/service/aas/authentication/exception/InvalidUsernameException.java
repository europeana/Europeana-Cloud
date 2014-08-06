package eu.europeana.cloud.service.aas.authentication.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Invalid user name exception
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 * @since 06.08.2014
 */
public class InvalidUsernameException extends GenericException {

    private static final long serialVersionUID = 2743985314014225235L;

    /**
     * Creates a new instance of this class.
     *
     * @param e
     */
    public InvalidUsernameException(ErrorInfo e) {
        super(e);
    }

    /**
     * Creates a new instance of this class.
     *
     * @param errorInfo
     */
    public InvalidUsernameException(IdentifierErrorInfo errorInfo) {
        super(errorInfo);
    }
}
