package eu.europeana.cloud.service.dps.storm.topologies;

import java.util.Properties;

import eu.europeana.cloud.service.dps.storm.topologies.properties.ReadTopologyProperties;

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
		ReadTopologyProperties reader = new ReadTopologyProperties();
		reader.loadDefaultPropertyFile(defaultPropertyFile, topologyProperties);
		if (providedPropertyFile != "")
			reader.loadProvidedPropertyFile(providedPropertyFile, topologyProperties);
	}
}