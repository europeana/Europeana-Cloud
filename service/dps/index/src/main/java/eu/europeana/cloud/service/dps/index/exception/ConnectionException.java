package eu.europeana.cloud.service.dps.index.exception;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ConnectionException extends IndexerException
{
    /**
     * Constructs a ConnectionException with no specified detail message.
     */
    public ConnectionException() 
    {
        super();
    }

    /**
     * Constructs a ConnectionException with the specified detail message.
     * 
     * @param message the detail message
     */
    public ConnectionException(String message) 
    {
        super(message);
    }

    /**
     * Constructs a ConnectionException with the specified detail message.
     * 
     * @param message the detailed message
     * @param e  exception
     */
    public ConnectionException(String message, Exception e) 
    {
        super(message, e);
    }
    
     /**
     * Constructs a ConnectionException with the specified Throwable.
     * 
     * @param cause  the cause
     */
    public ConnectionException(Throwable cause) 
    {
        super(cause);
    }
}
