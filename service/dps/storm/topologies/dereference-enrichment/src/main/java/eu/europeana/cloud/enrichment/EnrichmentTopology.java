package eu.europeana.cloud.enrichment;

import eu.europeana.cloud.enrichment.bolts.EnrichmentBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;

/**
 * Created by Tarek on 1/24/2018.
 */
public class EnrichmentTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "enrichment-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentTopology.class);

  public EnrichmentTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
  }

  public StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    List<String> spoutNames = TopologyHelper.addSpouts(builder, TopologiesNames.ENRICHMENT_TOPOLOGY, topologyProperties);

    ReadFileBolt readFileBolt = new ReadFileBolt(
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );
    WriteRecordBolt writeRecordBolt = new WriteRecordBolt(
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );
    RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));
    EnrichmentBolt enrichmentBolt = new EnrichmentBolt(
        topologyProperties.getProperty(DEREFERENCE_SERVICE_URL),
        topologyProperties.getProperty(ENRICHMENT_ENTITY_MANAGEMENT_URL),
        topologyProperties.getProperty(ENRICHMENT_ENTITY_API_URL),
        topologyProperties.getProperty(ENRICHMENT_ENTITY_API_KEY));

    // TOPOLOGY STRUCTURE!

    TopologyHelper.addSpoutShuffleGrouping(spoutNames,
        builder.setBolt(RETRIEVE_FILE_BOLT, readFileBolt, (getAnInt(RETRIEVE_FILE_BOLT_PARALLEL)))
               .setNumTasks((getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))));

    builder.setBolt(ENRICHMENT_BOLT, enrichmentBolt,
               (getAnInt(ENRICHMENT_BOLT_PARALLEL)))
           .setNumTasks(
               (getAnInt(ENRICHMENT_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

    builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
               (getAnInt(WRITE_BOLT_PARALLEL)))
           .setNumTasks(
               (getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(ENRICHMENT_BOLT, new ShuffleGrouping());

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
               .fieldsGrouping(RETRIEVE_FILE_BOLT, NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(ENRICHMENT_BOLT, NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(WRITE_RECORD_BOLT, NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(REVISION_WRITER_BOLT, NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));

    return builder.createTopology();
  }


  private static int getAnInt(String propertyName) {
    return parseInt(topologyProperties.getProperty(propertyName));
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.ENRICHMENT_TOPOLOGY);

      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        EnrichmentTopology enrichmentTopology =
            new EnrichmentTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = enrichmentTopology.buildTopology();
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

