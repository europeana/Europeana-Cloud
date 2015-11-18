package eu.europeana.cloud.service.dps.storm.topologies;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This is the abstract eCloud topology for Apache Storm.
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 *
 */
public abstract class eCloudAbstractTopology {

	public static Properties topologyProperties;

	public eCloudAbstractTopology(String defaultPropertyFile, String providedPropertyFile) {
		topologyProperties = new Properties();
		loadDefaultPropertyFile(defaultPropertyFile);
		if (providedPropertyFile != "")
			loadProvidedPropertyFile(providedPropertyFile);
	}
	
	String getTopologyProperty(String propertyName) {
		return topologyProperties.getProperty(propertyName);
	}

	private void loadDefaultPropertyFile(String defaultPropertyFile) {
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

	private void loadProvidedPropertyFile(String fileName) {
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