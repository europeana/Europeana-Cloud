package eu.europeana.cloud.service.dps.exception;

import java.util.concurrent.ExecutionException;

public class RecordSubmissionToKafkaException extends RuntimeException{

    public RecordSubmissionToKafkaException(String message, ExecutionException e) {
        super(message,e);
    }
}
