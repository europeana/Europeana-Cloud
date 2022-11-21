package data.validator.properities;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class for reading eCloud topology properties.
 */
public class PropertyFileLoader {

  static final Logger LOGGER = LoggerFactory.getLogger(PropertyFileLoader.class);


  public static void loadPropertyFile(String defaultPropertyFile, String providedPropertyFile, Properties topologyProperties) {
    try {
      PropertyFileLoader reader = new PropertyFileLoader();
      reader.loadDefaultPropertyFile(defaultPropertyFile, topologyProperties);
      if (providedPropertyFile != null) {
        reader.loadProvidedPropertyFile(providedPropertyFile, topologyProperties);
      }
    } catch (FileNotFoundException e) {
      LOGGER.error("ERROR while reading the properties file ", e);
    } catch (IOException e) {
      LOGGER.error("ERROR while reading the properties file ", e);
    }
  }

  public void loadDefaultPropertyFile(String defaultPropertyFile, Properties topologyProperties) throws IOException {
    InputStream propertiesInputStream = Thread.currentThread()
                                              .getContextClassLoader().getResourceAsStream(defaultPropertyFile);
    if (propertiesInputStream == null) {
      throw new FileNotFoundException();
    }
    topologyProperties.load(propertiesInputStream);

  }

  public void loadProvidedPropertyFile(String fileName, Properties topologyProperties) throws IOException {
    FileInputStream fileInput = new FileInputStream(fileName);
    topologyProperties.load(fileInput);
    fileInput.close();
  }
}
