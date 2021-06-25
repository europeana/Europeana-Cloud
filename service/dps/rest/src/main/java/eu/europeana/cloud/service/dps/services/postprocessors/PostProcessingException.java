package eu.europeana.cloud.service.dps.services.postprocessors;

public class PostProcessingException extends RuntimeException{

    public PostProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
