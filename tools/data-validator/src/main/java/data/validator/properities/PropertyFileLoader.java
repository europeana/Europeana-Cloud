package data.validator.properities;

import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Utility class for reading eCloud topology properties.
 */
public class PropertyFileLoader {
    final static Logger LOGGER = Logger.getLogger(PropertyFileLoader.class);


    public static void loadPropertyFile(String defaultPropertyFile, String providedPropertyFile, Properties topologyProperties) {
        try {
            PropertyFileLoader reader = new PropertyFileLoader();
            reader.loadDefaultPropertyFile(defaultPropertyFile, topologyProperties);
            if (providedPropertyFile != null)
                reader.loadProvidedPropertyFile(providedPropertyFile, topologyProperties);
        } catch (FileNotFoundException e) {
            LOGGER.error("ERROR while reading the properties file " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error("ERROR while reading the properties file " + e.getMessage());
        }
    }

    public void loadDefaultPropertyFile(String defaultPropertyFile, Properties topologyProperties) throws IOException {
        InputStream propertiesInputStream = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(defaultPropertyFile);
        if (propertiesInputStream == null)
            throw new FileNotFoundException();
        topologyProperties.load(propertiesInputStream);

    }

    public void loadProvidedPropertyFile(String fileName, Properties topologyProperties) throws IOException {
        FileInputStream fileInput = new FileInputStream(fileName);
        topologyProperties.load(fileInput);
        fileInput.close();
    }
}
