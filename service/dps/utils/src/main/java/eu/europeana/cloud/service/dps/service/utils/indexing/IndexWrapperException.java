package eu.europeana.cloud.service.dps.service.utils.indexing;

public class IndexWrapperException extends RuntimeException {

  public IndexWrapperException(String message, Exception e) {
    super(message, e);
  }
}
