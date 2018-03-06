package eu.europeana.cloud.service.dps.storm.topologies.properties;

import java.io.*;
import java.util.Properties;

/**
 * Utility class for reading eCloud topology properties.
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 */
public class PropertyFileLoader {

    public static void loadPropertyFile(String defaultPropertyFile, String providedPropertyFile, Properties topologyProperties) {
        try {
            PropertyFileLoader reader = new PropertyFileLoader();
            reader.loadDefaultPropertyFile(defaultPropertyFile, topologyProperties);
            if (!providedPropertyFile.equals(""))
                reader.loadProvidedPropertyFile(providedPropertyFile, topologyProperties);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadDefaultPropertyFile(String defaultPropertyFile, Properties topologyProperties) throws FileNotFoundException, IOException {
        InputStream propertiesInputStream = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(defaultPropertyFile);
        if (propertiesInputStream == null)
            throw new FileNotFoundException();
        topologyProperties.load(propertiesInputStream);

    }

    public void loadProvidedPropertyFile(String fileName, Properties topologyProperties) throws FileNotFoundException, IOException {

        File file = new File(fileName);
        FileInputStream fileInput = new FileInputStream(file);
        topologyProperties.load(fileInput);
        fileInput.close();

    }
}
