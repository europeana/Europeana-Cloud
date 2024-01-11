package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_ACCESSKEY;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_BUCKET;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_ENDPOINT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_SECRETKEY;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_ENRICHMENT_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_OBJECT_PROCESSOR_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_OBJECT_PROCESSOR_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.PARSE_FILE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RESOURCE_PROCESSING_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.WRITE_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.WRITE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.EDM_ENRICHMENT_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.EDM_OBJECT_PROCESSOR_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.NOTIFICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.PARSE_FILE_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RESOURCE_PROCESSING_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.REVISION_WRITER_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.WRITE_RECORD_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;
import static java.lang.Integer.parseInt;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
import eu.europeana.cloud.service.dps.storm.io.ParseFileForMediaBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
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
 * Created by Tarek on 12/14/2018.
 */
public class MediaTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "media-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(MediaTopology.class);

  public MediaTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
  }

  public final StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    List<String> spoutNames = TopologyHelper.addMediaSpouts(builder, TopologiesNames.MEDIA_TOPOLOGY, topologyProperties);

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
    AmazonClient amazonClient = new AmazonClient(
        topologyProperties.getProperty(AWS_CREDENTIALS_ACCESSKEY),
        topologyProperties.getProperty(AWS_CREDENTIALS_SECRETKEY),
        topologyProperties.getProperty(AWS_CREDENTIALS_ENDPOINT),
        topologyProperties.getProperty(AWS_CREDENTIALS_BUCKET)
    );

    TopologyHelper.addSpoutFieldGrouping(spoutNames,
        builder.setBolt(EDM_OBJECT_PROCESSOR_BOLT, new EDMObjectProcessorBolt(
                       createCassandraProperties(topologyProperties),
                       topologyProperties.getProperty(MCS_URL),
                       topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                       topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD),
                       amazonClient),
                   (getAnInt(EDM_OBJECT_PROCESSOR_BOLT_PARALLEL)))
               .setNumTasks((getAnInt(EDM_OBJECT_PROCESSOR_BOLT_NUMBER_OF_TASKS)))
        , StormTupleKeys.THROTTLING_GROUPING_ATTRIBUTE);

    builder.setBolt(PARSE_FILE_BOLT, new ParseFileForMediaBolt(
                   createCassandraProperties(topologyProperties),
                   topologyProperties.getProperty(MCS_URL),
                   topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                   topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)),
               (getAnInt(PARSE_FILE_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(EDM_OBJECT_PROCESSOR_BOLT, new ShuffleGrouping());

    builder.setBolt(RESOURCE_PROCESSING_BOLT,
               new ResourceProcessingBolt(createCassandraProperties(topologyProperties), amazonClient),
               (getAnInt(RESOURCE_PROCESSING_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS)))
           .fieldsGrouping(PARSE_FILE_BOLT, new Fields(StormTupleKeys.THROTTLING_GROUPING_ATTRIBUTE));

    builder.setBolt(EDM_ENRICHMENT_BOLT, new EDMEnrichmentBolt(createCassandraProperties(topologyProperties),
                   topologyProperties.getProperty(MCS_URL),
                   topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                   topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)),
               (getAnInt(EDM_ENRICHMENT_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS)))
           .fieldsGrouping(RESOURCE_PROCESSING_BOLT, new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY))
           .fieldsGrouping(EDM_OBJECT_PROCESSOR_BOLT, EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME,
               new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY));

    builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
               (getAnInt(WRITE_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(EDM_ENRICHMENT_BOLT, new ShuffleGrouping());

    builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
               (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(WRITE_RECORD_BOLT, new ShuffleGrouping());

    TopologyHelper.addSpoutsGroupingToNotificationBolt(spoutNames,
        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                       Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                       topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                       topologyProperties.getProperty(CASSANDRA_USERNAME),
                       topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                   getAnInt(NOTIFICATION_BOLT_PARALLEL))
               .setNumTasks(
                   (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
               .fieldsGrouping(EDM_OBJECT_PROCESSOR_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(PARSE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(RESOURCE_PROCESSING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(EDM_ENRICHMENT_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));

    return builder.createTopology();
  }

  private static int getAnInt(String propertyName) {
    return parseInt(topologyProperties.getProperty(propertyName));
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.MEDIA_TOPOLOGY);

      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        MediaTopology mediaTopology = new MediaTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = mediaTopology.buildTopology();
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
