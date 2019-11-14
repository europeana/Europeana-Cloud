package eu.europeana.cloud.service.dps.oaipmh;

/**
 * @author krystian.
 */
public class HarvesterException extends Exception {
    public HarvesterException(String message, Throwable cause) {
        super(message, cause);
    }

    public HarvesterException(String message) {
        super(message);
    }
}
