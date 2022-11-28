package eu.europeana.cloud.service.dps.storm.topologies.properties;

import com.google.common.base.Throwables;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for reading eCloud topology properties.
 */
public class PropertyFileLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyFileLoader.class);

  public static void loadPropertyFile(String defaultPropertyFilename, String providedPropertyFilename, Properties properties) {
    try {
      PropertyFileLoader reader = new PropertyFileLoader();
      if (!"".equals(defaultPropertyFilename)) {
        reader.loadDefaultPropertyFile(defaultPropertyFilename, properties);
      }
      if (!"".equals(providedPropertyFilename)) {
        reader.loadProvidedPropertyFile(providedPropertyFilename, properties);
      }
    } catch (IOException e) {
      LOGGER.error(Throwables.getStackTraceAsString(e));
    }
  }

  void loadDefaultPropertyFile(String defaultPropertyFilename, Properties properties) throws IOException {
    InputStream propertiesInputStream = Thread.currentThread().getContextClassLoader()
                                              .getResourceAsStream(defaultPropertyFilename);
    if (propertiesInputStream == null) {
      throw new FileNotFoundException(defaultPropertyFilename);
    }
    properties.load(propertiesInputStream);

  }

  void loadProvidedPropertyFile(String fileName, Properties properties) throws IOException {
    try (FileInputStream fileInput = new FileInputStream(fileName)) {
      properties.load(fileInput);
    }
  }
}
