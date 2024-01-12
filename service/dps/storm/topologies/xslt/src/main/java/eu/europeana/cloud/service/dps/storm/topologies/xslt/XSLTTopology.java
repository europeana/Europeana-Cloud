package eu.europeana.cloud.service.dps.storm.topologies.xslt;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.WRITE_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.WRITE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.XSLT_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.XSLT_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.NOTIFICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RETRIEVE_FILE_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.REVISION_WRITER_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.WRITE_RECORD_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.XSLT_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;
import static java.lang.Integer.parseInt;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt.XsltBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.List;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the XSLT transformation topology for Apache Storm. The topology reads from the cloud, download an XSLT sheet from a
 * remote server, apply it to each record read and save it back to the cloud.
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 */
public class XSLTTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "xslt-topology-config.properties";

  private static final Logger LOGGER = LoggerFactory.getLogger(XSLTTopology.class);

  public XSLTTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.XSLT_TOPOLOGY, topologyProperties);
  }

  public StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    List<String> spoutNames = TopologyHelper.addSpouts(builder, TopologiesNames.XSLT_TOPOLOGY, topologyProperties);

    ReadFileBolt readFileBolt = new ReadFileBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );
    WriteRecordBolt writeRecordBolt = new WriteRecordBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );
    RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );

    // TOPOLOGY STRUCTURE!

    TopologyHelper.addSpoutShuffleGrouping(spoutNames,
        builder.setBolt(RETRIEVE_FILE_BOLT, readFileBolt, (getAnInt(RETRIEVE_FILE_BOLT_PARALLEL)))
               .setNumTasks((getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))));

    builder.setBolt(XSLT_BOLT, new XsltBolt(createCassandraProperties(topologyProperties)),
               (getAnInt(XSLT_BOLT_PARALLEL)))
           .setNumTasks(
               (getAnInt(XSLT_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

    builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
               (getAnInt(WRITE_BOLT_PARALLEL)))
           .setNumTasks(
               (getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(XSLT_BOLT, new ShuffleGrouping());

    builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
               (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
           .setNumTasks(
               (getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(WRITE_RECORD_BOLT, new ShuffleGrouping());

    TopologyHelper.addSpoutsGroupingToNotificationBolt(spoutNames,
        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                       getAnInt(CASSANDRA_PORT),
                       topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                       topologyProperties.getProperty(CASSANDRA_USERNAME),
                       topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                   getAnInt(NOTIFICATION_BOLT_PARALLEL))
               .setNumTasks(
                   (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
               .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(XSLT_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));

    return builder.createTopology();
  }

  private static int getAnInt(String parseTasksBoltParallel) {
    return parseInt(topologyProperties.getProperty(parseTasksBoltParallel));
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.XSLT_TOPOLOGY);
      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        XSLTTopology xsltTopology = new XSLTTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = xsltTopology.buildTopology();
        Config config = buildConfig(topologyProperties);
        LOGGER.info("Submitting '{}'...", topologyProperties.getProperty(TOPOLOGY_NAME));
        TopologySubmitter.submitTopology(topologyProperties.getProperty(TOPOLOGY_NAME), config, stormTopology);
      } else {
        LOGGER.error("Invalid number of parameters");
      }
    } catch (Exception e) {
      LOGGER.error("General error while setting up topology", e);
    }
  }
}
