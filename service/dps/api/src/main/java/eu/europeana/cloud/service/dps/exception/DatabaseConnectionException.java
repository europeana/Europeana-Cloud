package eu.europeana.cloud.service.dps.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

public class DatabaseConnectionException extends GenericException {
    /**
     * Creates a new instance of this class.
     * @param e
     */
    public DatabaseConnectionException(ErrorInfo e){
        super(e);
    }

    /**
     * Creates a new instance of this class.
     * @param errorInfo
     */
    public DatabaseConnectionException(IdentifierErrorInfo errorInfo) {
        super(errorInfo);
    }
}
