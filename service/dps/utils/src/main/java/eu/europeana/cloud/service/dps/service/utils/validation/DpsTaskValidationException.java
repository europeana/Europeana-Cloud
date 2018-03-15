package eu.europeana.cloud.service.dps.service.utils.validation;

public class DpsTaskValidationException extends Exception {

    public DpsTaskValidationException(String message){
        super(message);
    }

    public DpsTaskValidationException(String message, Throwable cause){
        super(message,cause);
    }
}