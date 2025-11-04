package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import static eu.europeana.cloud.service.dps.storm.StormTupleKeys.INPUT_FILES_TUPLE_KEY;
import static eu.europeana.cloud.service.dps.storm.StormTupleKeys.THROTTLING_GROUPING_ATTRIBUTE;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_ACCESSKEY;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_BUCKET;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_ENDPOINT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.AWS_CREDENTIALS_SECRETKEY;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_ENRICHMENT_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_OBJECT_PROCESSOR_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.EDM_OBJECT_PROCESSOR_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.PARSE_FILE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RESOURCE_PROCESSING_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.EDM_ENRICHMENT_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.EDM_OBJECT_PROCESSOR_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.PARSE_FILE_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RESOURCE_PROCESSING_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.io.ParseFileForMediaBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
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
    TopologyPropertiesValidator.validateFor(TopologiesNames.MEDIA_TOPOLOGY, topologyProperties);
  }

  public final StormTopology buildTopology() {
    AmazonClient amazonClient = new AmazonClient(
        topologyProperties.getProperty(AWS_CREDENTIALS_ACCESSKEY),
        topologyProperties.getProperty(AWS_CREDENTIALS_SECRETKEY),
        topologyProperties.getProperty(AWS_CREDENTIALS_ENDPOINT),
        topologyProperties.getProperty(AWS_CREDENTIALS_BUCKET)
    );

    EDMObjectProcessorBolt edmObjectProcessorBolt = new EDMObjectProcessorBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD),
        amazonClient);

    ParseFileForMediaBolt parseFileBolt = new ParseFileForMediaBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));

    ResourceProcessingBolt resourceProcessingBolt = new ResourceProcessingBolt(createCassandraProperties(topologyProperties),
        amazonClient);

    EDMEnrichmentBolt edmEnrichmentBolt = new EDMEnrichmentBolt(createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));

    return new ECloudTopologyPipeline(TopologiesNames.MEDIA_TOPOLOGY, topologyProperties)
        .addBolt(EDM_OBJECT_PROCESSOR_BOLT, edmObjectProcessorBolt, EDM_OBJECT_PROCESSOR_BOLT_PARALLEL,
            EDM_OBJECT_PROCESSOR_BOLT_NUMBER_OF_TASKS, THROTTLING_GROUPING_ATTRIBUTE)
        .addBolt(PARSE_FILE_BOLT, parseFileBolt, PARSE_FILE_BOLT_PARALLEL, PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS)
        .addBolt(RESOURCE_PROCESSING_BOLT, resourceProcessingBolt, RESOURCE_PROCESSING_BOLT_PARALLEL,
            RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS, THROTTLING_GROUPING_ATTRIBUTE)
        .addBolt(EDM_ENRICHMENT_BOLT, edmEnrichmentBolt, EDM_ENRICHMENT_BOLT_PARALLEL, EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS,
            INPUT_FILES_TUPLE_KEY)
        .withAdditionalFieldGrouping(EDM_OBJECT_PROCESSOR_BOLT, EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME,
            INPUT_FILES_TUPLE_KEY)
        .addWriteRecordBolt("media_topology")
        .addRevisionWriterBolt()
        .buildTopology();
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.MEDIA_TOPOLOGY);

      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        MediaTopology mediaTopology = new MediaTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = mediaTopology.buildTopology();
        Config config = buildConfig(topologyProperties);
        String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);
        LOGGER.info("Submitting '{}'...", topologyName);
        TopologySubmitter.submitTopology(topologyName, config, stormTopology);
      } else {
        LOGGER.error("Invalid number of parameters");
      }
    } catch (Exception e) {
      LOGGER.error("General error while setting up topology", e);
    }
  }
}
