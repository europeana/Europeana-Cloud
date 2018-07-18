package eu.europeana.cloud.service.dps.examples.util;

import eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants;
import org.apache.storm.Config;


import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;

/**
 * Created by Tarek on 2/23/2018.
 */
public class TopologyConfigBuilder {
    public static Config buildConfig() {
        Config conf = new Config();
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.setNumAckers(0);
        conf.put(CASSANDRA_HOSTS, TopologyConstants.CASSANDRA_HOSTS);
        conf.put(CASSANDRA_PORT, TopologyConstants.CASSANDRA_PORT);
        conf.put(CASSANDRA_KEYSPACE_NAME, TopologyConstants.CASSANDRA_KEYSPACE_NAME);
        conf.put(CASSANDRA_USERNAME, TopologyConstants.CASSANDRA_USERNAME);
        conf.put(CASSANDRA_SECRET_TOKEN, TopologyConstants.CASSANDRA_SECRET_TOKEN);
        return conf;
    }
}
