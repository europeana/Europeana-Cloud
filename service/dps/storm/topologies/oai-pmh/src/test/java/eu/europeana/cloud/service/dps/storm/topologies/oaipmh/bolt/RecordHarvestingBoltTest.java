package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.harvesting.commons.IdentifierSupplier;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecord;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.Supplier;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

/**
 * Tests for {@link RecordHarvestingBolt}
 */

public class RecordHarvestingBoltTest {

  @Mock
  private OutputCollector outputCollector;

  @Mock
  private OaiHarvester harvester;

  @Spy
  private IdentifierSupplier identifierSupplier;

  @InjectMocks
  private final RecordHarvestingBolt recordHarvestingBolt = new RecordHarvestingBolt(new CassandraProperties());

  private static InputStream getFileContentAsStream(String name) {
    return RecordHarvestingBoltTest.class.getResourceAsStream(name);
  }

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void harvestingForAllParametersSpecified() throws IOException, HarvesterException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);

    OaiRecord oaiRecord = new OaiRecord(new OaiRecordHeader("id", false, Instant.now()), fileContent("/sampleEDMRecord.xml"));
    when(harvester.harvestRecord(any(), anyString())).thenReturn(oaiRecord);
    StormTaskTuple task = taskWithAllNeededParameters();
    StormTaskTuple spiedTask = spy(task);

    //when
    recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifySuccessfulEmit();
    verify(spiedTask).setFileData(Mockito.any(InputStream.class));
  }

  @Test
  public void shouldHarvestRecordInEDMAndExtractIdentifiers() throws IOException, HarvesterException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);

    OaiRecord oaiRecord = new OaiRecord(new OaiRecordHeader("id", false, Instant.now()), fileContent("/sampleEDMRecord.xml"));
    when(harvester.harvestRecord(any(), anyString())).thenReturn(oaiRecord);
    StormTaskTuple task = taskWithAllNeededParameters();
    StormTaskTuple spiedTask = spy(task);

    //when
    recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifySuccessfulEmit();

    verify(spiedTask).setFileData(Mockito.any(InputStream.class));
    assertEquals("http://more.locloud.eu/object/DCU/24927017",
        spiedTask.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
    assertEquals("/2020739_Ag_EU_CARARE_2Cultur/object_DCU_24927017",
        spiedTask.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
  }

  private Supplier<byte[]> fileContent(String fileName) {
    InputStream fileContentAsStream = getFileContentAsStream(fileName);
    return () -> {
      try {
        return fileContentAsStream.readAllBytes();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    };
  }

  @Test
  public void shouldEmitErrorOnHarvestingExceptionWhenCannotExctractEuropeanaIdFromEDM() throws HarvesterException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);

    OaiRecord oaiRecord = new OaiRecord(new OaiRecordHeader("id", false, Instant.now()), fileContent("/corruptedEDMRecord.xml"));
    when(harvester.harvestRecord(any(), anyString())).thenReturn(oaiRecord);
    StormTaskTuple task = taskWithAllNeededParameters();
    StormTaskTuple spiedTask = spy(task);

    //when
    recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifyErrorEmit();
  }

  @Test
  public void shouldEmitErrorOnHarvestingException() throws HarvesterException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);

    when(harvester.harvestRecord(any(), anyString())).thenThrow(new HarvesterException("Some!"));
    StormTaskTuple task = taskWithAllNeededParameters();
    StormTaskTuple spiedTask = spy(task);

    //when
    recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifyErrorEmit();
  }

  @Test
  public void harvestingForEmptyUrl() {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple task = taskWithoutResourceUrl();

    //when
    recordHarvestingBolt.execute(anchorTuple, task);

    //then
    verifyErrorEmit();
  }

  @Test
  public void harvestingForEmptyRecordId() {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple task = taskWithoutRecordId();

    //when
    recordHarvestingBolt.execute(anchorTuple, task);

    //then
    verifyErrorEmit();
  }

  @Test
  public void harvestForEmptyPrefix() {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple task = taskWithoutPrefix();

    //when
    recordHarvestingBolt.execute(anchorTuple, task);

    //then
    verifyErrorEmit();
  }

  private StormTaskTuple taskWithAllNeededParameters() {
    StormTaskTuple task = new StormTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
    task.setSourceDetails(details);
    task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
    task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
    task.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, "oaiIdentifier");
    task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, "2020739_Ag_EU_CARARE_2Culture");
    return task;
  }

  private StormTaskTuple taskWithoutResourceUrl() {
    StormTaskTuple task = new StormTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
    task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
    task.setSourceDetails(details);
    return task;
  }

  private StormTaskTuple taskWithoutRecordId() {
    StormTaskTuple task = new StormTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
    task.setSourceDetails(details);
    task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
    task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
    task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
    return task;
  }

  private StormTaskTuple taskWithoutPrefix() {
    StormTaskTuple task = new StormTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
    task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
    task.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, "oaiIdentifier");
    task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
    task.setSourceDetails(details);
    return task;
  }

  /**
   * Checks if emit to standard stream occured
   */
  private void verifySuccessfulEmit() {
    verify(outputCollector, times(1)).emit(Mockito.any(Tuple.class), Mockito.anyList());
    verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.any(Tuple.class), Mockito.anyList());
  }

  /**
   * Checks if emit to error stream occured
   */
  private void verifyErrorEmit() {

    verify(outputCollector, times(1)).emit(eq("NotificationStream"), Mockito.any(Tuple.class), Mockito.anyList());
    verify(outputCollector, times(0)).emit(Mockito.any(Tuple.class), Mockito.anyList());
  }
}
