package eu.europeana.cloud.service.dps.storm.topologies.validation.topology;

import static org.junit.Assert.assertEquals;

import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.StormTopology;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValidationTopologyTest {
  private static final int DEFAULT_PROPERTIES_BOLT_PARALLELISM = 2;
  private static final int DEFAULT_PROPERTIES_SPOUT_PARALLELISM = 1;

  @Test
  public void shouldProperlyBuildValidationTopology(){
    ValidationTopology xsltTopology = new ValidationTopology("defaultValidationTopologyConfig.properties", "",
        "defaultValidationConfig.properties", "");
    StormTopology topology = xsltTopology.buildTopology();

    assertEquals(6, topology.get_bolts_size());
    assertEquals(4, topology.get_spouts_size());
    topology.get_spouts().values().forEach(spoutSpec -> {
      String jsonConf = spoutSpec.get_common().get_json_conf();
      Assert.assertTrue(jsonConf.contains("\"config.bootstrap.servers\":\"2.2.2.2\""));
      assertEquals(2, spoutSpec.get_common().get_streams_size());
      assertEquals(0, spoutSpec.get_common().get_inputs_size());
      assertEquals(DEFAULT_PROPERTIES_SPOUT_PARALLELISM, spoutSpec.get_common().get_parallelism_hint());
    });

    ComponentCommon retrieveFileBoltCommon = topology.get_bolts().get(TopologyHelper.RETRIEVE_FILE_BOLT).get_common();
    assertEquals(4, retrieveFileBoltCommon.get_inputs_size());
    assertEquals(2, retrieveFileBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, retrieveFileBoltCommon.get_parallelism_hint());

    ComponentCommon statisticsBoltCommon = topology.get_bolts().get(TopologyHelper.STATISTICS_BOLT).get_common();
    assertEquals(1, statisticsBoltCommon.get_inputs_size());
    assertEquals(2, statisticsBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, statisticsBoltCommon.get_parallelism_hint());

    ComponentCommon notificationBoltCommon = topology.get_bolts().get(TopologyHelper.NOTIFICATION_BOLT).get_common();
    assertEquals(9, notificationBoltCommon.get_inputs_size());
    assertEquals(0, notificationBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, notificationBoltCommon.get_parallelism_hint());

    ComponentCommon revisionWriterBoltCommon = topology.get_bolts().get(TopologyHelper.REVISION_WRITER_BOLT).get_common();
    assertEquals(1, revisionWriterBoltCommon.get_inputs_size());
    assertEquals(2, revisionWriterBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, revisionWriterBoltCommon.get_parallelism_hint());

    ComponentCommon writeRecordBoltCommon = topology.get_bolts().get(TopologyHelper.WRITE_RECORD_BOLT).get_common();
    assertEquals(1, writeRecordBoltCommon.get_inputs_size());
    assertEquals(2, writeRecordBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, writeRecordBoltCommon.get_parallelism_hint());

    ComponentCommon validationBoltCommon = topology.get_bolts().get(TopologyHelper.VALIDATION_BOLT).get_common();
    assertEquals(1, validationBoltCommon.get_inputs_size());
    assertEquals(2, validationBoltCommon.get_streams_size());
    assertEquals(DEFAULT_PROPERTIES_BOLT_PARALLELISM, validationBoltCommon.get_parallelism_hint());
  }
}
