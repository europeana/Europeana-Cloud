package eu.europeana.cloud.service.dps.exception;

import java.util.concurrent.ExecutionException;

public class KafkaSubmissionException extends RuntimeException {

  public KafkaSubmissionException(String message, ExecutionException e) {
    super(message, e);
  }
}
