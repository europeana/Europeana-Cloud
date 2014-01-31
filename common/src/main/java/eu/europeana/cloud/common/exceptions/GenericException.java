package eu.europeana.cloud.common.exceptions;

import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Generic Exception
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class GenericException extends Exception {
    private static final long   serialVersionUID = -6146223626718871100L;
    
    private final IdentifierErrorInfo errorInfo;

    /**
     * Creates a new instance of this class.
     * 
     * @param e
     */
    public GenericException(ErrorInfo e) {
        super(e.getDetails());
        errorInfo=null;
    }

    /**
     * Creates a new instance of this class.
     * 
     * @param errorInfo
     */
    public GenericException(IdentifierErrorInfo errorInfo) {
        super(errorInfo.getErrorInfo().getDetails());
        this.errorInfo = errorInfo;
    }

    /**
     * Retrieve the error information that caused the exception happen
     * 
     * @return The error information
     */
    public IdentifierErrorInfo getErrorInfo() {
        return this.errorInfo;
    }
}
