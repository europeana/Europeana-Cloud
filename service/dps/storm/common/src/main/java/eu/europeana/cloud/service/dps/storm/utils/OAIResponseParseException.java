package eu.europeana.cloud.service.dps.storm.utils;

/**
 * <code>OAIResponseParseException</code> represents an error in an parsing OAI response.</p>
 */
public class OAIResponseParseException extends Exception{
    public OAIResponseParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public OAIResponseParseException(String message) {
        super(message);
    }
}
