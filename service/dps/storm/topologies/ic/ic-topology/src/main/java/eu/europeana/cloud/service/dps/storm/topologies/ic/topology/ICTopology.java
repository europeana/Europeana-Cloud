package eu.europeana.cloud.service.dps.storm.topologies.ic.topology;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.spout.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
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
;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;


/**
 * This is the Image conversion topology . The topology reads from the cloud,
 * apply Kakadu conversion to each record which was read and save it back to the
 * cloud.
 */
@Deprecated
public class ICTopology {

    private static Properties topologyProperties;
    private static final String TOPOLOGY_PROPERTIES_FILE = "ic-topology-config.properties";

    private static final Logger LOGGER = LoggerFactory.getLogger(ICTopology.class);

    public ICTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology(String icTopic, String ecloudMcsAddress) {

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);

        MCSReaderSpout mcsReaderSpout = getMcsReaderSpout(topologyProperties, icTopic, ecloudMcsAddress);

        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout(SPOUT, mcsReaderSpout,
                (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt,
                (getAnInt(RETRIEVE_FILE_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(SPOUT, new ShuffleGrouping());

        builder.setBolt(IC_BOLT, new IcBolt(),
                (getAnInt(IC_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(IC_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
                (getAnInt(WRITE_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(IC_BOLT, new ShuffleGrouping());

        builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
                (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(WRITE_RECORD_BOLT, new ShuffleGrouping());


        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                (getAnInt(ADD_TO_DATASET_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(REVISION_WRITER_BOLT, new ShuffleGrouping());


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(SPOUT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(IC_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
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

                ICTopology icTopology = new ICTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);
                // kafka topic == topology name
                String kafkaTopic = topologyName;
                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = icTopology.buildTopology(kafkaTopic, ecloudMcsAddress);
                Config config = configureTopology(topologyProperties);
                StormSubmitter.submitTopology(topologyName, config, stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }
    }
}

