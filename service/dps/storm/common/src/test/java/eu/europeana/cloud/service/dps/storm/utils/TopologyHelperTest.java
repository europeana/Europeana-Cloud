package eu.europeana.cloud.service.dps.storm.utils;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.BOOTSTRAP_SERVERS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.FETCH_MAX_BYTES;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.INPUT_ZOOKEEPER_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MAX_POLL_RECORDS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MAX_SPOUT_PENDING;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MAX_TASK_PARALLELISM;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MESSAGE_TIMEOUT_IN_SECONDS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NIMBUS_SEEDS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.SPOUT_SLEEP_MS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.STORM_ZOOKEEPER_ADDRESS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.THRIFT_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPICS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.WORKER_COUNT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.SPOUT_NAME_PREFIX;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.addSpoutFieldGrouping;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.addSpoutShuffleGrouping;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.addSpoutsGroupingToNotificationBolt;
import static org.apache.storm.Config.TOPOLOGY_KRYO_REGISTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class TopologyHelperTest {


  public static final String MAX_POLL_RECORDS_VALUE = "1";
  public static final String MAX_TASK_PARALLELISM_VALUE = "2";
  public static final String MAX_SPOUT_PENDING_VALUE = "3";
  public static final String SPOUT_SLEEP_MS_VALUE = "4";
  public static final String SPOUT_SLEEP_EVERY_VALUE = "5";
  public static final String MESSAGE_TIMEOUT_IN_SECONDS_VALUE = "6";
  private static final String INPUT_ZOOKEEPER_PORT_VALUE = "7";
  public static final String FETCH_MAX_BYTES_VALUE = "8";
  public static final String THRIFT_PORT_VALUE = "11";
  public static final String INPUT_ZOOKEEPER_ADDRESS_VALUE = "12";
  public static final String CASSANDRA_PORT_VALUE = "13";
  public static final String NIMBUS_SEED_VALUE = "test_seed";
  public static final String STORM_ZOOKEEPER_ADDRESS_VALUE = "0.0.0.0";
  public static final String CASSANDRA_HOSTS_VALUE = "1.1.1.1,2.2.2.2,3.3.3.3";
  public static final String BOOTSTRAP_SERVERS_VALUE = "4.4.4.4";
  public static final String CASSANDRA_USERNAME_VALUE = "cassandra_username";
  public static final String CASSANDRA_SECRET_TOKEN_VALUE = "cassandra_token";
  public static final String CASSANDRA_KEYSPACE_NAME_VALUE = "cassandra_keyspace_name";
  public static final String TOPICS_VALUE = "test_topic_1,test_topic_2,test_topic_3";
  public static final String STREAM_NAME = "test_stream_name";
  public static final String FIELD_NAME = "test_field_name";

  @Mock
  private Properties mockTopologyEssentialProperties;

  public static final String WORKER_COUNT_VALUE = "1";
  public static final List<String> SPOUT_NAMES = List.of(new String []{
      SPOUT_NAME_PREFIX + 1,
      SPOUT_NAME_PREFIX + 2,
      SPOUT_NAME_PREFIX + 3,
      SPOUT_NAME_PREFIX + 4,
      SPOUT_NAME_PREFIX + 5
  });

  @Before
  public void setUp() {
    mockTopologyEssentialProperties = mock(Properties.class);
  }

  @Test
  public void shouldProperlyLoadSpoutConfigParameters() {
    stubSpoutSettingsProperties();
    stubCassandraProperties();
    stubForNonStaticModeProperties();
    stubMisc();

    SpoutConfigParameters configParameters = TopologyHelper.transformProperties(mockTopologyEssentialProperties);

    assertEquals(CASSANDRA_KEYSPACE_NAME_VALUE, configParameters.getCassandraKeyspace());
    assertEquals(CASSANDRA_HOSTS_VALUE, configParameters.getCassandraHosts());
    assertEquals(Integer.valueOf(CASSANDRA_PORT_VALUE), configParameters.getCassandraPort());
    assertEquals(CASSANDRA_SECRET_TOKEN_VALUE, configParameters.getCassandraSecretToken());
    assertEquals(CASSANDRA_USERNAME_VALUE, configParameters.getCassandraUsername());

    assertEquals(Integer.valueOf(MAX_POLL_RECORDS_VALUE), configParameters.getMaxPollRecords());
    assertEquals(Integer.valueOf(MAX_TASK_PARALLELISM_VALUE), configParameters.getMaxTaskParallelism());
    assertEquals(Integer.valueOf(MAX_SPOUT_PENDING_VALUE), configParameters.getMaxSpoutPending());
    assertEquals(Integer.valueOf(SPOUT_SLEEP_MS_VALUE), configParameters.getSpoutSleepMilliseconds());
    assertEquals(Integer.valueOf(SPOUT_SLEEP_EVERY_VALUE), configParameters.getSpoutSleepEveryNIterations());
    assertEquals(Integer.valueOf(MESSAGE_TIMEOUT_IN_SECONDS_VALUE), configParameters.getMessageTimeoutInSeconds());
    assertEquals(Integer.valueOf(FETCH_MAX_BYTES_VALUE), configParameters.getFetchMaxBytes());
    assertEquals(Integer.valueOf(THRIFT_PORT_VALUE), configParameters.getNimbusThriftPort());
    assertEquals(INPUT_ZOOKEEPER_PORT_VALUE, configParameters.getInputZookeeperPort());
    assertEquals(INPUT_ZOOKEEPER_ADDRESS_VALUE, configParameters.getInputZookeeperAddress());
    assertEquals(Collections.singletonList(NIMBUS_SEED_VALUE), configParameters.getNimbusSeeds());
    assertEquals(TOPICS_VALUE, configParameters.getTopics());

  }

  @Test
  public void shouldProperlyHandleLoadingNullSpoutConfigParameters() {
    SpoutConfigParameters configParameters = TopologyHelper.transformProperties(mockTopologyEssentialProperties);

    assertNull(configParameters.getCassandraKeyspace());
    assertNull(configParameters.getCassandraHosts());
    assertNull(configParameters.getCassandraPort());
    assertNull(configParameters.getCassandraSecretToken());
    assertNull(configParameters.getCassandraUsername());

    assertNull(configParameters.getMaxPollRecords());
    assertNull(configParameters.getMaxTaskParallelism());
    assertNull(configParameters.getMaxSpoutPending());
    assertNull(configParameters.getSpoutSleepMilliseconds());
    assertNull(configParameters.getSpoutSleepEveryNIterations());
    assertNull(configParameters.getMessageTimeoutInSeconds());
    assertNull(configParameters.getFetchMaxBytes());
    assertNull(configParameters.getNimbusThriftPort());
    assertNull(configParameters.getInputZookeeperPort());
    assertNull(configParameters.getInputZookeeperAddress());
    assertNull(configParameters.getTopics());
    assertEquals(Collections.singletonList(null), configParameters.getNimbusSeeds());

  }

  @Test
  public void shouldProperlyLoadCassandraDefaultValuesWhenParametersAreNotProvidedForStaticConfig() {
    Config staticConfig = TopologyHelper.buildConfig(mockTopologyEssentialProperties, true);

    assertEquals(TopologyDefaultsConstants.DEFAULT_CASSANDRA_USERNAME, staticConfig.get(CASSANDRA_USERNAME));
    assertEquals(TopologyDefaultsConstants.DEFAULT_CASSANDRA_PORT, staticConfig.get(CASSANDRA_PORT));
    assertEquals(TopologyDefaultsConstants.DEFAULT_CASSANDRA_SECRET_TOKEN, staticConfig.get(CASSANDRA_SECRET_TOKEN));
    assertEquals(TopologyDefaultsConstants.DEFAULT_CASSANDRA_HOSTS, staticConfig.get(CASSANDRA_HOSTS));
    assertEquals(TopologyDefaultsConstants.DEFAULT_CASSANDRA_KEYSPACE_NAME, staticConfig.get(CASSANDRA_KEYSPACE_NAME));
  }

  @Test
  public void shouldProperlyLoadCassandraDefaultValuesWhenParametersAreNotProvidedForNonStaticConfig() {
    stubForNonStaticModeProperties();
    Config nonStaticConfig = TopologyHelper.buildConfig(mockTopologyEssentialProperties, false);

    assertNull(nonStaticConfig.get(CASSANDRA_KEYSPACE_NAME));
    assertNull(nonStaticConfig.get(CASSANDRA_USERNAME));
    assertNull(nonStaticConfig.get(CASSANDRA_PORT));
    assertNull(nonStaticConfig.get(CASSANDRA_SECRET_TOKEN));
    assertNull(nonStaticConfig.get(CASSANDRA_HOSTS));
  }

  @Test
  public void shouldProperlyLoadMiscParametersWhenPropertyPresent() {
    stubMisc();
    Config config = TopologyHelper.buildConfig(mockTopologyEssentialProperties, true);

    assertEquals(Integer.valueOf(SPOUT_SLEEP_MS_VALUE), config.get(SPOUT_SLEEP_MS));
    assertEquals(Integer.valueOf(SPOUT_SLEEP_EVERY_VALUE), config.get(SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS));
    assertEquals(config.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY), FastCancelingSpoutWaitStrategy.class.getName());
    assertEquals(Integer.valueOf(MAX_SPOUT_PENDING_VALUE), config.get("topology.max.spout.pending"));
    assertEquals(Integer.valueOf(MESSAGE_TIMEOUT_IN_SECONDS_VALUE), config.get("topology.message.timeout.secs"));
    assertTrue((Boolean) config.get("topology.debug"));

    assertNotNull(config.get(TOPOLOGY_KRYO_REGISTER));
  }


  @Test
  public void shouldProperlyLoadDefaultMiscParametersWhenPropertyNotPresent() {
    stubForNonStaticModeProperties();
    Config config = TopologyHelper.buildConfig(mockTopologyEssentialProperties, false);

    assertEquals(TopologyDefaultsConstants.DEFAULT_SPOUT_SLEEP_MS, config.get(SPOUT_SLEEP_MS));
    assertEquals(TopologyDefaultsConstants.DEFAULT_SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS,
        config.get(SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS));
    assertEquals(config.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY), FastCancelingSpoutWaitStrategy.class.getName());
    assertEquals(TopologyDefaultsConstants.DEFAULT_MAX_SPOUT_PENDING, config.get("topology.max.spout.pending"));
    assertEquals(TopologyDefaultsConstants.DEFAULT_TUPLE_PROCESSING_TIME, config.get("topology.message.timeout.secs"));
    assertFalse((Boolean) config.get("topology.debug"));

    assertNotNull(config.get(TOPOLOGY_KRYO_REGISTER));
  }

  @Test
  public void shouldProperlyAddSpout(){
    stubMisc();
    stubCassandraProperties();



    SpoutDeclarer spoutDeclarer = mock(SpoutDeclarer.class);
    TopologyBuilder builder = mock(TopologyBuilder.class);
    when(builder.setSpout(anyString(), any(ECloudSpout.class), anyInt())).thenReturn(spoutDeclarer);
    when(spoutDeclarer.setNumTasks(anyInt())).thenReturn(spoutDeclarer);

    List<String> result_adding_spout = TopologyHelper.addSpouts(builder, TOPICS_VALUE, mockTopologyEssentialProperties);
    assertEquals(TOPICS_VALUE.split(",").length, result_adding_spout.size());
    for (int i = 0; i < TOPICS_VALUE.split(",").length; i++) {
      assertTrue(result_adding_spout.contains(SPOUT_NAME_PREFIX + (i+1)));
    }


    List<String> result_adding_media_spout = TopologyHelper.addMediaSpouts(builder, TOPICS_VALUE, mockTopologyEssentialProperties);
    assertEquals(TOPICS_VALUE.split(",").length, result_adding_media_spout.size());
    for (int i = 0; i < TOPICS_VALUE.split(",").length; i++) {
      assertTrue(result_adding_media_spout.contains(SPOUT_NAME_PREFIX + (i+1)));
    }
  }

  @Test
  public void shouldProperlyAddSpoutShuffleGrouping(){
    BoltDeclarer boltDeclarer = mock(BoltDeclarer.class);
    when(boltDeclarer.shuffleGrouping(anyString())).thenReturn(boltDeclarer);

    addSpoutShuffleGrouping(SPOUT_NAMES, boltDeclarer);

    verify(boltDeclarer, times(SPOUT_NAMES.size())).shuffleGrouping(anyString());
  }

  @Test
  public void shouldProperlyAddSpoutFieldGrouping(){
    BoltDeclarer boltDeclarer = mock(BoltDeclarer.class);
    when(boltDeclarer.fieldsGrouping(anyString(), any(Fields.class))).thenReturn(boltDeclarer);

    addSpoutFieldGrouping(SPOUT_NAMES, boltDeclarer, FIELD_NAME);

    verify(boltDeclarer, times(SPOUT_NAMES.size())).fieldsGrouping(anyString(), any(Fields.class));
  }

  @Test
  public void shouldProperlyAddSpoutFieldGroupingToNotificationBolt(){
    BoltDeclarer boltDeclarer = mock(BoltDeclarer.class);
    when(boltDeclarer.fieldsGrouping(anyString(), anyString(), any(Fields.class))).thenReturn(boltDeclarer);

    addSpoutsGroupingToNotificationBolt(SPOUT_NAMES, boltDeclarer);

    verify(boltDeclarer, times(SPOUT_NAMES.size())).fieldsGrouping(anyString(), eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), eq(new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));
  }

  private void stubSpoutSettingsProperties() {
    when(mockTopologyEssentialProperties.getProperty(SPOUT_SLEEP_MS)).thenReturn(SPOUT_SLEEP_MS_VALUE);
    when(mockTopologyEssentialProperties.getProperty(SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS)).thenReturn(SPOUT_SLEEP_EVERY_VALUE);
    when(mockTopologyEssentialProperties.getProperty(MESSAGE_TIMEOUT_IN_SECONDS)).thenReturn(MESSAGE_TIMEOUT_IN_SECONDS_VALUE);
  }

  private void stubCassandraProperties() {
    when(mockTopologyEssentialProperties.getProperty(CASSANDRA_HOSTS)).thenReturn(CASSANDRA_HOSTS_VALUE);
    when(mockTopologyEssentialProperties.getProperty(CASSANDRA_PORT)).thenReturn(CASSANDRA_PORT_VALUE);
    when(mockTopologyEssentialProperties.getProperty(CASSANDRA_USERNAME)).thenReturn(CASSANDRA_USERNAME_VALUE);
    when(mockTopologyEssentialProperties.getProperty(CASSANDRA_SECRET_TOKEN)).thenReturn(CASSANDRA_SECRET_TOKEN_VALUE);
    when(mockTopologyEssentialProperties.getProperty(CASSANDRA_KEYSPACE_NAME)).thenReturn(CASSANDRA_KEYSPACE_NAME_VALUE);
  }

  private void stubForNonStaticModeProperties() {
    when(mockTopologyEssentialProperties.getProperty(WORKER_COUNT)).thenReturn(WORKER_COUNT_VALUE);
    when(mockTopologyEssentialProperties.getProperty(MAX_TASK_PARALLELISM)).thenReturn(MAX_TASK_PARALLELISM_VALUE);
    when(mockTopologyEssentialProperties.getProperty(THRIFT_PORT)).thenReturn(THRIFT_PORT_VALUE);
    when(mockTopologyEssentialProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS)).thenReturn(INPUT_ZOOKEEPER_ADDRESS_VALUE);
    when(mockTopologyEssentialProperties.getProperty(INPUT_ZOOKEEPER_PORT)).thenReturn(INPUT_ZOOKEEPER_PORT_VALUE);
    when(mockTopologyEssentialProperties.getProperty(NIMBUS_SEEDS)).thenReturn(NIMBUS_SEED_VALUE);
    when(mockTopologyEssentialProperties.getProperty(STORM_ZOOKEEPER_ADDRESS)).thenReturn(STORM_ZOOKEEPER_ADDRESS_VALUE);
  }

  private void stubMisc() {
    when(mockTopologyEssentialProperties.getProperty(SPOUT_SLEEP_MS)).thenReturn(SPOUT_SLEEP_MS_VALUE);
    when(mockTopologyEssentialProperties.getProperty(SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS)).thenReturn(SPOUT_SLEEP_EVERY_VALUE);
    when(mockTopologyEssentialProperties.getProperty(FETCH_MAX_BYTES)).thenReturn(FETCH_MAX_BYTES_VALUE);
    when(mockTopologyEssentialProperties.getProperty(MAX_POLL_RECORDS)).thenReturn(MAX_POLL_RECORDS_VALUE);
    when(mockTopologyEssentialProperties.getProperty(BOOTSTRAP_SERVERS)).thenReturn(BOOTSTRAP_SERVERS_VALUE);
    when(mockTopologyEssentialProperties.getProperty(MAX_SPOUT_PENDING)).thenReturn(MAX_SPOUT_PENDING_VALUE);
    when(mockTopologyEssentialProperties.getProperty(MESSAGE_TIMEOUT_IN_SECONDS)).thenReturn(MESSAGE_TIMEOUT_IN_SECONDS_VALUE);
    when(mockTopologyEssentialProperties.getProperty(TOPICS)).thenReturn(TOPICS_VALUE);
  }

}
