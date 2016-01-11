package eu.europeana.cloud.service.dps.storm.topologies.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class for reading eCloud topology properties.
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 *
 */
public class PropertyFileLoader {
	
	public static void loadPropertyFile(String defaultPropertyFile, String providedPropertyFile, Properties topologyProperties) {
		topologyProperties = new Properties();
		PropertyFileLoader reader = new PropertyFileLoader();
		reader.loadDefaultPropertyFile(defaultPropertyFile, topologyProperties);
		if (providedPropertyFile != "")
			reader.loadProvidedPropertyFile(providedPropertyFile, topologyProperties);
	}
		
	public void loadDefaultPropertyFile(String defaultPropertyFile, Properties topologyProperties) {
		try {
			InputStream propertiesInputStream = Thread.currentThread()
				    .getContextClassLoader().getResourceAsStream(defaultPropertyFile);
			topologyProperties.load(propertiesInputStream);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadProvidedPropertyFile(String fileName, Properties topologyProperties) {
		try {
			File file = new File(fileName);
			FileInputStream fileInput = new FileInputStream(file);
			topologyProperties.load(fileInput);
			fileInput.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
