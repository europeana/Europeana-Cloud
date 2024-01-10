package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import static org.junit.Assert.assertEquals;

import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.StormTopology;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MediaTopologyTest {

  private static final int DEFAULT_PROPERTIES_BOLT_PARALLELISM = 2;
  private static final int DEFAULT_PROPERTIES_SPOUT_PARALLELISM = 1;

  @Test
  public void shouldProperlyBuildMediaTopology(){
    MediaTopology mediaTopology = new MediaTopology("defaultMediaTopologyConfig.properties", "");
    StormTopology topology = mediaTopology.buildTopology();

    assertEquals(7, topology.get_bolts_size());
    assertEquals(4, topology.get_spouts_size());
    topology.get_spouts().values().forEach(spoutSpec -> {
      String jsonConf = spoutSpec.get_common().get_json_conf();
      Assert.assertTrue(jsonConf.contains("\"config.bootstrap.servers\":\"2.2.2.2\""));
      assertEquals(2, spoutSpec.get_common().get_streams_size());
      assertEquals(0, spoutSpec.get_common().get_inputs_size());
      assertEquals(DEFAULT_PROPERTIES_SPOUT_PARALLELISM, spoutSpec.get_common().get_parallelism_hint());
    });

    ComponentCommon edmEnrichmentBoltCommon = topology.get_bolts().get(TopologyHelper.EDM_ENRICHMENT_BOLT).get_common();
    assertEquals(2, edmEnrichmentBoltCommon.get_inputs_size());
    assertEquals(2, edmEnrichmentBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, edmEnrichmentBoltCommon.get_parallelism_hint());

    ComponentCommon parseFileBoltCommon = topology.get_bolts().get(TopologyHelper.PARSE_FILE_BOLT).get_common();
    assertEquals(1, parseFileBoltCommon.get_inputs_size());
    assertEquals(2, parseFileBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, parseFileBoltCommon.get_parallelism_hint());

    ComponentCommon writeRecordBoltCommon = topology.get_bolts().get(TopologyHelper.WRITE_RECORD_BOLT).get_common();
    assertEquals(1, writeRecordBoltCommon.get_inputs_size());
    assertEquals(2, writeRecordBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, writeRecordBoltCommon.get_parallelism_hint());

    ComponentCommon notificationBoltCommon = topology.get_bolts().get(TopologyHelper.NOTIFICATION_BOLT).get_common();
    assertEquals(10, notificationBoltCommon.get_inputs_size());
    assertEquals(0, notificationBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, notificationBoltCommon.get_parallelism_hint());

    ComponentCommon resourceProcessingBoltCommon = topology.get_bolts().get(TopologyHelper.RESOURCE_PROCESSING_BOLT).get_common();
    assertEquals(1, resourceProcessingBoltCommon.get_inputs_size());
    assertEquals(2, resourceProcessingBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, resourceProcessingBoltCommon.get_parallelism_hint());

    ComponentCommon EDMObjectProcessorBoltCommon = topology.get_bolts().get(TopologyHelper.EDM_OBJECT_PROCESSOR_BOLT).get_common();
    assertEquals(4, EDMObjectProcessorBoltCommon.get_inputs_size());
    assertEquals(3, EDMObjectProcessorBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, EDMObjectProcessorBoltCommon.get_parallelism_hint());

    ComponentCommon revisionWriterBoltCommon = topology.get_bolts().get(TopologyHelper.REVISION_WRITER_BOLT).get_common();
    assertEquals(1, revisionWriterBoltCommon.get_inputs_size());
    assertEquals(2, revisionWriterBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, revisionWriterBoltCommon.get_parallelism_hint());
  }
}
