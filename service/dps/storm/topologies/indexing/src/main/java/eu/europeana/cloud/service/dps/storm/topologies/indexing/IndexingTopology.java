package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import com.google.common.base.Throwables;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
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

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTopology.class);

    private static Properties topologyProperties = new Properties();
    private static Properties indexingProperties = new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "indexing-topology-config.properties";
    private static final String INDEXING_PROPERTIES_FILE = "indexing.properties";
    public static final String SUCCESS_MESSAGE = "Record is indexed correctly";

    private IndexingTopology(String defaultPropertyFile, String providedPropertyFile, String defaultIndexingPropertiesFile, String providedIndexingPropertiesFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        PropertyFileLoader.loadPropertyFile(defaultIndexingPropertiesFile, providedIndexingPropertiesFile, indexingProperties);
    }

    private StormTopology buildTopology(String indexingTopic, String ecloudMcsAddress) {

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);

        MCSReaderSpout mcsReaderSpout = getMcsReaderSpout(topologyProperties, indexingTopic, ecloudMcsAddress);
        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout(SPOUT, mcsReaderSpout,
                getAnInt(KAFKA_SPOUT_PARALLEL))
                .setNumTasks(getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS));

        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt,
                getAnInt(RETRIEVE_FILE_BOLT_PARALLEL))
                .setNumTasks(getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))
                .customGrouping(SPOUT, new ShuffleGrouping());

        builder.setBolt(INDEXING_BOLT, new IndexingBolt(indexingProperties),
                getAnInt(INDEXING_BOLT_PARALLEL))
                .setNumTasks(getAnInt(INDEXING_BOLT_NUMBER_OF_TASKS))
                .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

        builder.setBolt(REVISION_WRITER_BOLT, new ValidationRevisionWriter(ecloudMcsAddress, SUCCESS_MESSAGE),
                getAnInt(REVISION_WRITER_BOLT_PARALLEL))
                .setNumTasks(getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS))
                .customGrouping(INDEXING_BOLT, new ShuffleGrouping());

        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        getAnInt(CASSANDRA_PORT),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS))
                .fieldsGrouping(SPOUT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(INDEXING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));
        return builder.createTopology();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling indexing topology");

            if (args.length <= 2) {

                String providedIndexingPropertiesFile = "";
                String providedPropertyFile = "";
                if (args.length == 1)
                    providedPropertyFile = args[0];
                else if (args.length == 2) {
                    providedPropertyFile = args[0];
                    providedIndexingPropertiesFile = args[1];
                }
                IndexingTopology indexingTopology = new IndexingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile, INDEXING_PROPERTIES_FILE, providedIndexingPropertiesFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);
                // kafka topic == topology name
                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = indexingTopology.buildTopology(topologyName, ecloudMcsAddress);

                Config config = configureTopology(topologyProperties);
                LOGGER.info("Submitting indexing topology");
                StormSubmitter.submitTopology(topologyName, config, stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }

    }

    private static int getAnInt(String propertyName) {
        return Integer.parseInt(topologyProperties.getProperty(propertyName));
    }
}
