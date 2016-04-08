package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions;

/**
 * Created by Tarek on 8/24/2015.
 */

/**
 * Thrown whene there is attempt to create a file which is already attached to a specific representation version.
 */
public class ConversionException extends ICSException {

    /**
     * Constructs a UnexpectedExtensionsException with no specified detail message.
     */
    public ConversionException() {
    }


    /**
     * Constructs a UnexpectedExtensionsException
     * with the specified detail message.
     *
     * @param message the detail message
     */
    public ConversionException(String message) {
        super(message);
    }


    /**
     * Constructs a UnexpectedExtensionsException
     * with the specified detail message.
     *
     * @param message the detail message
     * @param e       exception
     */

    public ConversionException(String message, Exception e) {
        super(message, e);
    }


}
