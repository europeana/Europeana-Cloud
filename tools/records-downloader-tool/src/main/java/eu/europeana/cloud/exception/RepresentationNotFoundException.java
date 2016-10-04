package eu.europeana.cloud.exception;


/**
 * Created by Tarek on 9/2/2016.
 */
public class RepresentationNotFoundException extends Exception {
    private static final long serialVersionUID = 1987750363232807009L;


    public RepresentationNotFoundException() {
    }

    /**
     * Constructs an RepresentationNotFoundException with the specified detail message.
     *
     * @param message the detail message
     */
    public RepresentationNotFoundException(String message) {
        super(message);
    }
}

