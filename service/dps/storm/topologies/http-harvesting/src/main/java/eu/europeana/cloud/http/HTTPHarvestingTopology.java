package eu.europeana.cloud.http;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DUPLICATES_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;

import eu.europeana.cloud.harvesting.DuplicatedRecordsProcessorBolt;
import eu.europeana.cloud.http.bolts.HttpHarvestedRecordCategorizationBolt;
import eu.europeana.cloud.http.bolts.HttpHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBoltForHarvesting;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
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
 * HttpHarvestingTopology main class where its definition is formulated
 * Created by Tarek on 3/22/2018.
 */
public class HTTPHarvestingTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "http-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(HTTPHarvestingTopology.class);

  /**
   *Main constructor for the {@link HTTPHarvestingTopology} class
   *
   * @param defaultPropertyFile   location of the file containing default topology properties
   * @param providedPropertyFile  location of the file containing topology properties that will override default properties
   */
  public HTTPHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
  }

  /**
   * Builds actual topology
   * @return created topology definition
   */
  public final StormTopology buildTopology() {
    String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
    String uisAddress = topologyProperties.getProperty(UIS_URL);

    TopologyBuilder builder = new TopologyBuilder();

    List<String> spoutNames = TopologyHelper.addSpouts(builder, TopologiesNames.HTTP_TOPOLOGY, topologyProperties);

    WriteRecordBolt writeRecordBolt = new HarvestingWriteRecordBolt(
        ecloudMcsAddress,
        uisAddress,
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));
    RevisionWriterBolt revisionWriterBolt = new RevisionWriterBoltForHarvesting(
        ecloudMcsAddress,
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));

    TopologyHelper.addSpoutShuffleGrouping(spoutNames,
        builder.setBolt(RECORD_HARVESTING_BOLT, new HttpHarvestingBolt(), (getAnInt(RECORD_HARVESTING_BOLT_PARALLEL)))
               .setNumTasks((getAnInt(RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS))));

    builder.setBolt(RECORD_CATEGORIZATION_BOLT, new HttpHarvestedRecordCategorizationBolt(prepareConnectionDetails()),
               (getAnInt(RECORD_HARVESTING_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(RECORD_HARVESTING_BOLT, new ShuffleGrouping());

    builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
               (getAnInt(WRITE_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(RECORD_CATEGORIZATION_BOLT, new ShuffleGrouping());

    builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
               (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(WRITE_RECORD_BOLT, new ShuffleGrouping());

    builder.setBolt(DUPLICATES_DETECTOR_BOLT, new DuplicatedRecordsProcessorBolt(
                            topologyProperties.getProperty(MCS_URL),
                            topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                            topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
                    ),
                    (getAnInt(DUPLICATES_BOLT_PARALLEL)))
            .setNumTasks((getAnInt(DUPLICATES_BOLT_NUMBER_OF_TASKS)))
            .fieldsGrouping(REVISION_WRITER_BOLT, new Fields(NotificationTuple.TASK_ID_FIELD_NAME));

    TopologyHelper.addSpoutsGroupingToNotificationBolt(spoutNames,
            builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                                    Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                                    topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                                    topologyProperties.getProperty(CASSANDRA_USERNAME),
                                    topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                            getAnInt(NOTIFICATION_BOLT_PARALLEL))
                    .setNumTasks(
                            (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
                    .fieldsGrouping(RECORD_HARVESTING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                            new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                    .fieldsGrouping(RECORD_CATEGORIZATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                            new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                    .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                            new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                    .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                            new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                    .fieldsGrouping(DUPLICATES_DETECTOR_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                            new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
    );

    return builder.createTopology();
  }

  private static int getAnInt(String propertyName) {
    return parseInt(topologyProperties.getProperty(propertyName));
  }

  private DbConnectionDetails prepareConnectionDetails() {
    return DbConnectionDetails.builder()
                              .hosts(topologyProperties.getProperty(CASSANDRA_HOSTS))
                              .port(getAnInt(CASSANDRA_PORT))
                              .keyspaceName(topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME))
                              .userName(topologyProperties.getProperty(CASSANDRA_USERNAME))
                              .password(topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN))
                              .build();
  }

  /**
   * Method executed by the Storm engine while deploying topology on the cluster
   * @param args list of all needed arguments (in thi case: location of the properties file)
   */
  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.HTTP_TOPOLOGY);
      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        HTTPHarvestingTopology httpHarvestingTopology = new HTTPHarvestingTopology(TOPOLOGY_PROPERTIES_FILE,
            providedPropertyFile);

        StormTopology stormTopology = httpHarvestingTopology.buildTopology();
        Config config = buildConfig(topologyProperties);
        LOGGER.info("Submitting '{}'...", topologyProperties.getProperty(TOPOLOGY_NAME));
        TopologySubmitter.submitTopology(topologyProperties.getProperty(TOPOLOGY_NAME), config, stormTopology);
      } else {
        LOGGER.error("Invalid number of parameters");
      }
    } catch (Exception e) {
      LOGGER.error("General error in HTTP harvesting topology", e);
    }
  }
}
