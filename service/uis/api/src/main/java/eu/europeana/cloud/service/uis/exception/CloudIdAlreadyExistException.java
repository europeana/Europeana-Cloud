package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * The unique identifier already exist exception.
 * 
 */
public class CloudIdAlreadyExistException extends GenericException {

    /**
     *
     */
    private static final long serialVersionUID = -381851449536503512L;

    /**
     * Creates a new instance of this class.
     * 
     * @param e
     *            ErrorInfo
     */
    public CloudIdAlreadyExistException(ErrorInfo e) {
	super(e);
    }

    /**
     * Creates a new instance of this class.
     * 
     * @param errorInfo
     *            ErrorInfo
     */
    public CloudIdAlreadyExistException(IdentifierErrorInfo errorInfo) {
	super(errorInfo);
    }

}
