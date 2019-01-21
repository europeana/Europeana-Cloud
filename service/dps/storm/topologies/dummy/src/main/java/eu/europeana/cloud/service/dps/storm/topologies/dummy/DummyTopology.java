package eu.europeana.cloud.service.dps.storm.topologies.dummy;

import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.topologies.dummy.bolt.DummyBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;

import com.google.common.base.Throwables;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;

public class DummyTopology {

    private static Properties topologyProperties;
    private static final String TOPOLOGY_PROPERTIES_FILE = "dummy-topology-config.properties";

    private static final Logger LOGGER = LoggerFactory.getLogger(DummyTopology.class);

    public DummyTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);

    }

    public StormTopology buildTopology(String dummyTopic, String ecloudMcsAddress) {

        MCSReaderSpout mcsReaderSpout = getMcsReaderSpout(topologyProperties, dummyTopic, ecloudMcsAddress);

        TopologyBuilder builder = new TopologyBuilder();


        // TOPOLOGY STRUCTURE!
        builder.setSpout(SPOUT, mcsReaderSpout,
                (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));


        builder.setBolt(DUMMY_BOLT, new DummyBolt(),
                (getAnInt(DUMMY_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(DUMMY_BOLT_NUMBER_OF_TASKS)));


        return builder.createTopology();
    }

    private static int getAnInt(String parseTasksBoltParallel) {
        return parseInt(topologyProperties.getProperty(parseTasksBoltParallel));
    }

    public static void main(String[] args) {
        try {

            if (args.length <= 1) {


                String providedPropertyFile = "";
                if (args.length == 1) {
                    providedPropertyFile = args[0];
                }

                DummyTopology dummyTopology = new DummyTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);

                // assuming kafka topic == topology name
                String kafkaTopic = topologyName;

                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = dummyTopology.buildTopology(
                        kafkaTopic,
                        ecloudMcsAddress);
                Config config = configureTopology(topologyProperties);
                StormSubmitter.submitTopology(topologyName, config, stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));


        }
    }
}