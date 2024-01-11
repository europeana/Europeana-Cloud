package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import static org.junit.Assert.assertEquals;

import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.StormTopology;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LinkCheckTopologyTest {
  private static final int DEFAULT_PROPERTIES_BOLT_PARALLELISM = 2;
  private static final int DEFAULT_PROPERTIES_SPOUT_PARALLELISM = 1;

  @Test
  public void shouldProperlyBuildLinkCheckTopology(){
    LinkCheckTopology linkCheckTopology = new LinkCheckTopology("defaultLinkCheckTopologyConfig.properties", "");
    StormTopology topology = linkCheckTopology.buildTopology();

    assertEquals(3, topology.get_bolts_size());
    assertEquals(4, topology.get_spouts_size());
    topology.get_spouts().values().forEach(spoutSpec -> {
      String jsonConf = spoutSpec.get_common().get_json_conf();
      Assert.assertTrue(jsonConf.contains("\"config.bootstrap.servers\":\"2.2.2.2\""));
      assertEquals(2, spoutSpec.get_common().get_streams_size());
      assertEquals(0, spoutSpec.get_common().get_inputs_size());
      assertEquals(DEFAULT_PROPERTIES_SPOUT_PARALLELISM, spoutSpec.get_common().get_parallelism_hint());
    });


    ComponentCommon parseFileBoltCommon = topology.get_bolts().get(TopologyHelper.PARSE_FILE_BOLT).get_common();
    assertEquals(4, parseFileBoltCommon.get_inputs_size());
    assertEquals(2, parseFileBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, parseFileBoltCommon.get_parallelism_hint());

    ComponentCommon notificationBoltCommon = topology.get_bolts().get(TopologyHelper.NOTIFICATION_BOLT).get_common();
    assertEquals(6, notificationBoltCommon.get_inputs_size());
    assertEquals(0, notificationBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, notificationBoltCommon.get_parallelism_hint());

    ComponentCommon linkCheckBoltCommon = topology.get_bolts().get(TopologyHelper.LINK_CHECK_BOLT).get_common();
    assertEquals(1, linkCheckBoltCommon.get_inputs_size());
    assertEquals(2, linkCheckBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, linkCheckBoltCommon.get_parallelism_hint());
  }
}
