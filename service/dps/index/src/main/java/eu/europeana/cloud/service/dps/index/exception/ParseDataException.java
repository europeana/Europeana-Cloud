package eu.europeana.cloud.service.dps.index.exception;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ParseDataException extends IndexerException
{
    /**
     * Constructs a ParseDataException with no specified detail message.
     */
    public ParseDataException() 
    {
        super();
    }

    /**
     * Constructs a ParseDataException with the specified detail message.
     * 
     * @param message the detail message
     */
    public ParseDataException(String message) 
    {
        super(message);
    }

    /**
     * Constructs a ParseDataException with the specified detail message.
     * 
     * @param message the detailed message
     * @param e  exception
     */
    public ParseDataException(String message, Exception e) 
    {
        super(message, e);
    }
    
     /**
     * Constructs a ParseDataException with the specified Throwable.
     * 
     * @param cause  the cause
     */
    public ParseDataException(Throwable cause) 
    {
        super(cause);
    }
}
