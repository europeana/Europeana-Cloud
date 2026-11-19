package eu.europeana.cloud.service.dps.storm.topologies.administration;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.ProcessingData;
import eu.europeana.cloud.service.dps.storm.tuple.common.RecordData;
import eu.europeana.cloud.service.dps.storm.tuple.common.TaskData;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class DatasetAdministrationBoltTest {

  private final int TASK_ID = 1;

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock(name = "uisClient")
  private UISClient uisClient;

  @Captor
  ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

  @InjectMocks
  private DatasetAdministrationBolt xsltBolt = new DatasetAdministrationBolt(new CassandraProperties(),
          "http://localhost:8080/uis",
          "user",
          "password");

  @Test
  void executeBolt() throws IOException, URISyntaxException, CloudException {

    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn("exampleCloudId");
    when(uisClient.createCloudId(anyString(), anyString())).thenReturn(cloudId);

    Tuple anchorTuple = mock(TupleImpl.class);
    String sampleXmlFileName = "xmlForTesting.xml";
    Path pathToFile = Paths.get(ClassLoader.getSystemResource(sampleXmlFileName).toURI());
    byte[] FILE_DATA = Files.readAllBytes(pathToFile);
    CommonTaskTuple tuple = new CommonTaskTuple(
            new TaskData(TASK_ID, TASK_NAME),
            new RecordData(SOURCE_VERSION_URL, FILE_DATA, true),
            new ProcessingData());
    tuple.setCloudId("sampleCloudId");
    tuple.addParameter(PluginParameterKeys.METIS_DATASET_ID,"xyz");
    tuple.addParameter(PluginParameterKeys.PROVIDER_ID,"exampleProvider");
    tuple.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER, new Date()));
    xsltBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(1)).emit(Mockito.any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    List<Values> allValues = captor.getAllValues();
    assertEmittedTuple(allValues, 3);
  }

  private void assertEmittedTuple(List<Values> allValues, int expectedParametersSize) {
    assertNotNull(allValues);
    assertEquals(1, allValues.size());

    //parameters assertion
    assertTrue(allValues.get(0).get(3) instanceof TaskData);
    var parameters = ((TaskData) allValues.get(0).get(3)).getParameters();
    assertNotNull(parameters);
    assertEquals(parameters.size(), expectedParametersSize);
    String cloudId = parameters.get(PluginParameterKeys.CLOUD_ID);
    assertNotNull(cloudId);
    assertEquals(cloudId, "exampleCloudId");
  }

}