package eu.europeana.cloud.service.dps.index.exception;

import eu.europeana.cloud.service.dps.exception.DpsException;

/**
 * Base class for all exceptions that may be thrown from the Index.
 * 
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexException extends DpsException
{
     /**
     * Constructs a IndexException with no specified detail message.
     */
    public IndexException() 
    {
        super();
    }

    /**
     * Constructs a IndexException with the specified detail message.
     * 
     * @param message the detail message
     */
    public IndexException(String message) 
    {
        super(message);
    }

    /**
     * Constructs a IndexException with the specified detail message.
     * 
     * @param message the detailed message
     * @param e  exception
     */
    public IndexException(String message, Exception e) 
    {
        super(message, e);
    }
    
     /**
     * Constructs a IndexException with the specified Throwable.
     * 
     * @param cause  the cause
     */
    public IndexException(Throwable cause) 
    {
        super(cause);
    }
}
