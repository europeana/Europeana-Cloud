package eu.europeana.cloud.service.dps.storm.topologies.properties;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by Tarek on 11/13/2015.
 */
public class PropertyFileLoaderTest {

	String TOPOLOGY_PROPERTIES_FILE = "test-config.properties";
	String PROVIDED_TOPOLOGY_PROPERTIES_FILE = "src/main/resources/test-config.properties";
	public static Properties topologyProperties;
	PropertyFileLoader reader;

	@Before
	public void init() {
		topologyProperties = new Properties();
		reader = new PropertyFileLoader();
	}

	@Test
	public void getDefaultPropertiesTest() {
		reader.loadDefaultPropertyFile(TOPOLOGY_PROPERTIES_FILE, topologyProperties);
		assertEquals(topologyProperties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME), "xslt_topology_1");
	}

	@Test
	public void getProvidedValueTest() {
		reader.loadProvidedPropertyFile(PROVIDED_TOPOLOGY_PROPERTIES_FILE, topologyProperties);
		assertEquals(topologyProperties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME), "xslt_topology_1");
	}
}