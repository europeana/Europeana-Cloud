package eu.europeana.cloud.service.dps.storm.io;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
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
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.UIS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.WRITE_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.WRITE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.NOTIFICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RETRIEVE_FILE_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.REVISION_WRITER_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.WRITE_RECORD_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;
import static java.lang.Integer.parseInt;

import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import java.util.List;
import java.util.Properties;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Builder for ECloud topologies. The builder always add ECloudSpouts - based on configuration and NotificationBolt. Rest of bolts
 * could add using add* methods, important is to execute these methods in the same order, as the bolts should be in the topology
 * graph. Automatically is created grouping between spout and first bolt, between subsequent bolts, and between all the nodes
 * (spouts and bolts) and the NotificationBolt. The class internally use TopologyBuilder from Storm library, which is more general
 * and has more possibilities to configure. And this class is dedicated for the eCloud topologies to simplify repeatable process
 * of configuring them using the mentioned Storm general builder. Because all the eCloud topologies have similar structure,
 * most of the code could be unified.
 */
public class ECloudTopologyBuilder {

  private final Properties topologyProperties;
  private final TopologyBuilder rawBuilder;

  private final BoltDeclarer notificationBolt;
  private final List<String> spoutNames;
  private String lastBoltName;
  private BoltDeclarer lastBolt;

  public ECloudTopologyBuilder(String topologyName, Properties topologyProperties) {
    this.topologyProperties = topologyProperties;
    createCassandraProperties(topologyProperties);
    rawBuilder = new TopologyBuilder();
    spoutNames = prepareSpouts(topologyProperties, topologyName);
    notificationBolt = prepareNotificationBolt(topologyProperties);
  }

  public ECloudTopologyBuilder addReadFileBolt() {
    ReadFileBolt readFileBolt = new ReadFileBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );

    addBolt(RETRIEVE_FILE_BOLT, readFileBolt, RETRIEVE_FILE_BOLT_PARALLEL, RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS);
    return this;
  }

  public ECloudTopologyBuilder addWriteRecordBolt() {
    WriteRecordBolt writeRecordBolt = new WriteRecordBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );
    addBolt(WRITE_RECORD_BOLT, writeRecordBolt, WRITE_BOLT_PARALLEL, WRITE_BOLT_NUMBER_OF_TASKS);
    return this;
  }

  public ECloudTopologyBuilder addHarvestingWriteRecordBolt() {
    WriteRecordBolt writeRecordBolt = new HarvestingWriteRecordBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(UIS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );
    addBolt(WRITE_RECORD_BOLT, writeRecordBolt, WRITE_BOLT_PARALLEL, WRITE_BOLT_NUMBER_OF_TASKS);
    return this;
  }

  public ECloudTopologyBuilder addRevisionWriterBolt() {
    RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));

    addBolt(REVISION_WRITER_BOLT, revisionWriterBolt, REVISION_WRITER_BOLT_PARALLEL, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);
    return this;
  }

  public ECloudTopologyBuilder addRevisionWriterBoltForHarvesting() {
    RevisionWriterBolt revisionWriterBolt = new RevisionWriterBoltForHarvesting(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
    );

    addBolt(REVISION_WRITER_BOLT, revisionWriterBolt, REVISION_WRITER_BOLT_PARALLEL, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);
    return this;
  }

  /**
   * Adds the bolt with a ShuffleGrouping tuples from the previous bolt, or spouts it the bolt is first added.
   *
   * @param boltName - bolt name used as key for grouping and for diagnostic
   * @param bolt - bolt instance
   * @param parallelismParamName - name of the parameter defining bolt parallelism, from the topology properties
   * @param taskCountParamName - name of the parameter defining number of bolt tasks, from the topology properties
   * @return this builder
   */
  public ECloudTopologyBuilder addBolt(String boltName, IRichBolt bolt, String parallelismParamName, String taskCountParamName) {
    BoltDeclarer declarer = rawBuilder.setBolt(boltName, bolt, getIntProperty(parallelismParamName))
                                      .setNumTasks(getIntProperty(taskCountParamName));
    if (lastBoltName != null) {
      declarer.customGrouping(lastBoltName, new ShuffleGrouping());
    } else {
      TopologyHelper.addSpoutShuffleGrouping(spoutNames, declarer);
    }

    notificationBolt.fieldsGrouping(boltName, NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME));
    lastBoltName = boltName;
    lastBolt = declarer;
    return this;
  }

  /**
   * Adds the bolt with FieldGrouping from previous bolt, or spouts for the first bolt.
   *
   * @param boltName - bolt name used as key for grouping and for diagnostic
   * @param bolt - bolt instance
   * @param parallelismParamName - name of the parameter defining bolt parallelism, from the topology properties
   * @param taskCountParamName - name of the parameter defining number of bolt tasks, from the topology properties
   * @param groupingFieldName - name of the field used by FieldGrouping
   * @return - this builder
   */
  public ECloudTopologyBuilder addBolt(String boltName, IRichBolt bolt, String parallelismParamName, String taskCountParamName,
      String groupingFieldName) {
    BoltDeclarer declarer = rawBuilder.setBolt(boltName, bolt, getIntProperty(parallelismParamName))
                                      .setNumTasks(getIntProperty(taskCountParamName));
    if (lastBoltName != null) {
      declarer.fieldsGrouping(lastBoltName, new Fields(groupingFieldName));
    } else {
      TopologyHelper.addSpoutFieldGrouping(spoutNames, declarer, groupingFieldName);
    }

    notificationBolt.fieldsGrouping(boltName, NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME));
    lastBoltName = boltName;
    lastBolt = declarer;
    return this;
  }

  public StormTopology build() {
    return rawBuilder.createTopology();
  }

  /**
   * Adds additional field grouping for the tuples from the stream of given name
   *
   * @param sourceBoltName - the name of the component (for example bolt), from which the last added bolt receives tuples
   * @param streamName - name of the stream
   * @param groupingFieldName - name of the field used by FieldGrouping
   * @return - this builder
   */
  public ECloudTopologyBuilder withAdditionalFieldGrouping(String sourceBoltName, String streamName, String groupingFieldName) {
    lastBolt.fieldsGrouping(sourceBoltName, streamName, new Fields(groupingFieldName));
    return this;
  }

  private BoltDeclarer prepareNotificationBolt(Properties topologyProperties) {
    return rawBuilder.setBolt(NOTIFICATION_BOLT,
                         new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                             getIntProperty(CASSANDRA_PORT),
                             topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                             topologyProperties.getProperty(CASSANDRA_USERNAME),
                             topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                         getIntProperty(NOTIFICATION_BOLT_PARALLEL))
                     .setNumTasks(
                         getIntProperty(NOTIFICATION_BOLT_NUMBER_OF_TASKS));
  }

  private List<String> prepareSpouts(Properties topologyProperties, String topologyName) {
    return TopologyHelper.addSpouts(rawBuilder, topologyName, topologyProperties);
  }

  private int getIntProperty(String propertyName) {
    return parseInt(topologyProperties.getProperty(propertyName));
  }

}
