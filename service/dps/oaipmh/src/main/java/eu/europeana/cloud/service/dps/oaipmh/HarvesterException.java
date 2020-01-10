package eu.europeana.cloud.service.dps.oaipmh;

/**
 * Exception that can be thrown as the result of a OAI-PMH harvesting issue.
 *
 * @author krystian.
 */
public class HarvesterException extends Exception {

    private static final long serialVersionUID = -2152193422749121097L;

    /**
     * Constructor.
     *
     * @param message The message of the exception.
     * @param cause The cause of the exception.
     */
    public HarvesterException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param message The message of the exception.
     */
    public HarvesterException(String message) {
        super(message);
    }
}
