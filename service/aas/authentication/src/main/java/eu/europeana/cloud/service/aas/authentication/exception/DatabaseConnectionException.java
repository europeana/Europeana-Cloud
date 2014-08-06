/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.europeana.cloud.service.aas.authentication.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Database connection exception
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 * @since 06.08.2014
 */
public class DatabaseConnectionException extends GenericException {

    private static final long serialVersionUID = 2743985314014225235L;

    /**
     * Creates a new instance of this class.
     *
     * @param e
     */
    public DatabaseConnectionException(ErrorInfo e) {
        super(e);
    }

    /**
     * Creates a new instance of this class.
     *
     * @param errorInfo
     */
    public DatabaseConnectionException(IdentifierErrorInfo errorInfo) {
        super(errorInfo);
    }
}
