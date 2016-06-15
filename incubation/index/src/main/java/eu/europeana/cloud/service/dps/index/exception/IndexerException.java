package eu.europeana.cloud.service.dps.index.exception;

import eu.europeana.cloud.service.dps.exception.DpsException;

/**
 * Base class for all exceptions that may be thrown from the Index.
 * 
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerException extends DpsException
{
    /**
     * Constructs a IndexException with no specified detail message.
     */
    public IndexerException() 
    {
        super();
    }

    /**
     * Constructs a IndexException with the specified detail message.
     * 
     * @param message the detail message
     */
    public IndexerException(String message) 
    {
        super(message);
    }

    /**
     * Constructs a IndexException with the specified detail message.
     * 
     * @param message the detailed message
     * @param e  exception
     */
    public IndexerException(String message, Exception e) 
    {
        super(message, e);
    }
    
     /**
     * Constructs a IndexException with the specified Throwable.
     * 
     * @param cause  the cause
     */
    public IndexerException(Throwable cause) 
    {
        super(cause);
    }
}
