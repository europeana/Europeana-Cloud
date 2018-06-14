package eu.europeana.cloud.service.dps.storm.topologies.properties;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Utility class for reading eCloud topology properties.
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 */
public class PropertyFileLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyFileLoader.class);

    public static void loadPropertyFile(String defaultPropertyFile, String providedPropertyFile, Properties topologyProperties) {
        try {
            PropertyFileLoader reader = new PropertyFileLoader();
            reader.loadDefaultPropertyFile(defaultPropertyFile, topologyProperties);
            if (!providedPropertyFile.equals(""))
                reader.loadProvidedPropertyFile(providedPropertyFile, topologyProperties);
        } catch (FileNotFoundException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        } catch (IOException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
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
