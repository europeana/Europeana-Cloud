package data.validator.properities;

import java.io.*;
import java.util.Properties;

/**
 * Utility class for reading eCloud topology properties.
 */
public class PropertyFileLoader {

    public static void loadPropertyFile(String defaultPropertyFile, String providedPropertyFile, Properties topologyProperties) {
        try {
            PropertyFileLoader reader = new PropertyFileLoader();
            reader.loadDefaultPropertyFile(defaultPropertyFile, topologyProperties);
            if (providedPropertyFile != null)
                reader.loadProvidedPropertyFile(providedPropertyFile, topologyProperties);
        } catch (FileNotFoundException e) {
            System.out.println("ERROR while reading the properties file " + e.getMessage());
        } catch (IOException e) {
            System.out.println("ERROR while reading the properties file " + e.getMessage());
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
        File file = new File(fileName);
        FileInputStream fileInput = new FileInputStream(file);
        topologyProperties.load(fileInput);
        fileInput.close();
    }
}
