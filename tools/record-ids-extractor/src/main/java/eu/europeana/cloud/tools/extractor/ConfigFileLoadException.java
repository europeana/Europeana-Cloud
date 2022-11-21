package eu.europeana.cloud.tools.extractor;

import org.apache.commons.configuration2.ex.ConfigurationException;

public class ConfigFileLoadException extends RuntimeException {

  public ConfigFileLoadException(String message, ConfigurationException e) {
    super(message, e);
  }

}
