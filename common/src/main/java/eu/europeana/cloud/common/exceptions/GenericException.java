package eu.europeana.cloud.common.exceptions;

import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;
import lombok.Getter;

/**
 * Generic Exception
 */
@Getter
public class GenericException extends Exception {
    private static final long serialVersionUID = -6146223626718871100L;
    
    private final IdentifierErrorInfo errorInfo;

    /**
     * Create new instance for given message
     */
    public GenericException(String detail) {
        this(detail, null);
    }

    /**
     * Creates a new instance for given {@link ErrorInfo}.
     */
    public GenericException(ErrorInfo errorInfo) {
        this(errorInfo.getDetails());
    }

    /**
     * Creates a new instance for given {@link IdentifierErrorInfo}.
     */
    public GenericException(IdentifierErrorInfo identifierErrorInfo) {
        this(identifierErrorInfo.getErrorInfo().getDetails(), identifierErrorInfo);
    }

    private GenericException(String detail, IdentifierErrorInfo errorInfo) {
        super(detail);
        this.errorInfo = errorInfo;
    }
}
