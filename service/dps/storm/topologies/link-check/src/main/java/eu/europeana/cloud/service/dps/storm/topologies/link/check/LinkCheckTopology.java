package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import com.google.common.base.Throwables;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
import eu.europeana.cloud.service.dps.storm.io.ParseFileBolt;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;

/**
 * Created by Tarek on 12/14/2018.
 */
public class LinkCheckTopology {
    private static Properties topologyProperties;
    private static final String TOPOLOGY_PROPERTIES_FILE = "link-check-topology-config.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkCheckTopology.class);

    public LinkCheckTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology(String mediaTopic, String ecloudMcsAddress) {

        TopologyBuilder builder = new TopologyBuilder();
        MCSReaderSpout mcsReaderSpout = getMcsReaderSpout(topologyProperties, mediaTopic, ecloudMcsAddress);


        builder.setSpout(SPOUT, mcsReaderSpout, (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks((getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(PARSE_FILE_BOLT, new ParseFileBolt(ecloudMcsAddress),
                (getAnInt(PARSE_FILE_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(SPOUT, new ShuffleGrouping());

        builder.setBolt(LINK_CHECK_BOLT, new LinkCheckBolt(),
                (getAnInt(LINK_CHECK_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(LINK_CHECK_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(PARSE_FILE_BOLT, new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY));


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(SPOUT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(PARSE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(LINK_CHECK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));


        return builder.createTopology();
    }

    private static int getAnInt(String propertyName) {
        return parseInt(topologyProperties.getProperty(propertyName));
    }

    public static void main(String[] args) {
        try {
            if (args.length <= 1) {

                String providedPropertyFile = "";
                if (args.length == 1) {
                    providedPropertyFile = args[0];
                }

                LinkCheckTopology linkCheckTopology = new LinkCheckTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);

                // assuming kafka topic == topology name
                String kafkaTopic = topologyName;
                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = linkCheckTopology.buildTopology(
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
