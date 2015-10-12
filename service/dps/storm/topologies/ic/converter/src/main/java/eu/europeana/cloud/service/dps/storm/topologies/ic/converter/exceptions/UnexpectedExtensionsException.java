package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions;

/**
 * Created by Tarek on 8/24/2015.
 */

/**
 * Thrown whene there is attempt to create a file which is already attached to a specific representation version.
 */
public class UnexpectedExtensionsException extends ICSException {

    /**
     * Constructs a UnexpectedExtensionsException with no specified detail message.
     */
    public UnexpectedExtensionsException() {
    }


    /**
     * Constructs a UnexpectedExtensionsException
     * with the specified detail message.
     *
     * @param message the detail message
     */
    public UnexpectedExtensionsException(String message) {
        super(message);
    }


    /**
     * Constructs a UnexpectedExtensionsException
     * with the specified detail message.
     *
     * @param message the detail message
     * @param e       exception
     */

    public UnexpectedExtensionsException(String message, Exception e) {
        super(message, e);
    }


}
