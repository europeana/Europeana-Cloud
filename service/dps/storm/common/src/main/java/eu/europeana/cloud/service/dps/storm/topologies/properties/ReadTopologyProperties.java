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
public class ReadTopologyProperties {

	public ReadTopologyProperties() {
		
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
	
	String getTopologyProperty(String propertyName, Properties topologyProperties) {
		return topologyProperties.getProperty(propertyName);
	}
}
