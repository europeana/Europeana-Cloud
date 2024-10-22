package eu.europeana.cloud.service.dps.storm.utils;

public class TopologyPropertiesException extends RuntimeException {

  public TopologyPropertiesException(String message) {
    super(message);
  }

  public TopologyPropertiesException(String message, Throwable e) {
    super(message, e);
  }
}
