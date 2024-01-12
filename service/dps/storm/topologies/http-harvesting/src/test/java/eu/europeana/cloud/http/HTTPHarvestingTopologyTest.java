package eu.europeana.cloud.http;

import static org.junit.Assert.assertEquals;

import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.StormTopology;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HTTPHarvestingTopologyTest {
  private static final int DEFAULT_PROPERTIES_BOLT_PARALLELISM = 2;
  private static final int DEFAULT_PROPERTIES_SPOUT_PARALLELISM = 1;

  @Test
  public void shouldProperlyBuildHTTPHarvestingTopology(){
    HTTPHarvestingTopology httpHarvestingTopology = new HTTPHarvestingTopology("defaultHTTPHarvestingTopologyConfig.properties", "");
    StormTopology topology = httpHarvestingTopology.buildTopology();

    assertEquals(6, topology.get_bolts_size());
    assertEquals(4, topology.get_spouts_size());
    topology.get_spouts().values().forEach(spoutSpec -> {
      String jsonConf = spoutSpec.get_common().get_json_conf();
      Assert.assertTrue(jsonConf.contains("\"config.bootstrap.servers\":\"2.2.2.2\""));
      assertEquals(2, spoutSpec.get_common().get_streams_size());
      assertEquals(0, spoutSpec.get_common().get_inputs_size());
      assertEquals(DEFAULT_PROPERTIES_SPOUT_PARALLELISM, spoutSpec.get_common().get_parallelism_hint());
    });

    ComponentCommon recordHarvestingBoltCommon = topology.get_bolts().get(TopologyHelper.RECORD_HARVESTING_BOLT).get_common();
    assertEquals(4, recordHarvestingBoltCommon.get_inputs_size());
    assertEquals(2, recordHarvestingBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, recordHarvestingBoltCommon.get_parallelism_hint());

    ComponentCommon recordCategorizationBoltCommon = topology.get_bolts().get(TopologyHelper.RECORD_CATEGORIZATION_BOLT).get_common();
    assertEquals(1, recordCategorizationBoltCommon.get_inputs_size());
    assertEquals(2, recordCategorizationBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, recordCategorizationBoltCommon.get_parallelism_hint());

    ComponentCommon writeRecordBoltCommon = topology.get_bolts().get(TopologyHelper.WRITE_RECORD_BOLT).get_common();
    assertEquals(1, writeRecordBoltCommon.get_inputs_size());
    assertEquals(2, writeRecordBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, writeRecordBoltCommon.get_parallelism_hint());

    ComponentCommon notificationBoltCommon = topology.get_bolts().get(TopologyHelper.NOTIFICATION_BOLT).get_common();
    assertEquals(9, notificationBoltCommon.get_inputs_size());
    assertEquals(0, notificationBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, notificationBoltCommon.get_parallelism_hint());

    ComponentCommon revisionWriterBoltCommon = topology.get_bolts().get(TopologyHelper.REVISION_WRITER_BOLT).get_common();
    assertEquals(1, revisionWriterBoltCommon.get_inputs_size());
    assertEquals(2, revisionWriterBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, revisionWriterBoltCommon.get_parallelism_hint());

    ComponentCommon duplicatesDetectorBoltCommon = topology.get_bolts().get(TopologyHelper.DUPLICATES_DETECTOR_BOLT).get_common();
    assertEquals(1, duplicatesDetectorBoltCommon.get_inputs_size());
    assertEquals(2, duplicatesDetectorBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, duplicatesDetectorBoltCommon.get_parallelism_hint());



  }
}
