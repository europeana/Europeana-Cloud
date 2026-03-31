package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.harvesting.commons.IdentifierSupplier;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecord;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link RecordHarvestingBolt}
 */
@ExtendWith(MockitoExtension.class)
class RecordHarvestingBoltTest {

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


  @Test
  void harvestingForAllParametersSpecified() throws IOException, HarvesterException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);

    OaiRecord oaiRecord = new OaiRecord(new OaiRecordHeader("id", false, Instant.now()), fileContent("/sampleEDMRecord.xml"));
    when(harvester.harvestRecord(any(), anyString())).thenReturn(oaiRecord);
    CommonTaskTuple task = taskWithAllNeededParameters();
    CommonTaskTuple spiedTask = spy(task);

    //when
    recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifySuccessfulEmit();
    verify(spiedTask).setFileData(Mockito.any(InputStream.class));
  }

  @Test
  void shouldHarvestRecordInEDMAndExtractIdentifiers() throws IOException, HarvesterException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);

    OaiRecord oaiRecord = new OaiRecord(new OaiRecordHeader("id", false, Instant.now()), fileContent("/sampleEDMRecord.xml"));
    when(harvester.harvestRecord(any(), anyString())).thenReturn(oaiRecord);
    CommonTaskTuple task = taskWithAllNeededParameters();
    CommonTaskTuple spiedTask = spy(task);

    //when
    recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifySuccessfulEmit();

    verify(spiedTask).setFileData(Mockito.any(InputStream.class));
    assertEquals("http://more.locloud.eu/object/DCU/24927017",
            spiedTask.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
    assertEquals("/2020739_Ag_EU_CARARE_2Cultur/object_DCU_24927017",
        spiedTask.getParameter(PluginParameterKeys.EUROPEANA_ID));
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
    void shouldEmitErrorOnHarvestingExceptionWhenCannotExtractEuropeanaIdFromEDM() throws HarvesterException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);

        OaiRecord oaiRecord = new OaiRecord(new OaiRecordHeader("id", false, Instant.now()), fileContent("/corruptedEDMRecord.xml"));
        when(harvester.harvestRecord(any(), anyString())).thenReturn(oaiRecord);
        CommonTaskTuple task = taskWithAllNeededParameters();
        CommonTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifyErrorEmit();
  }

    @Test
    void shouldEmitErrorOnHarvestingException() throws HarvesterException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);

        when(harvester.harvestRecord(any(), anyString())).thenThrow(new HarvesterException("Some!"));
        CommonTaskTuple task = taskWithAllNeededParameters();
        CommonTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(anchorTuple, spiedTask);

    //then
    verifyErrorEmit();
  }

    @Test
    void harvestingForEmptyUrl() {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        CommonTaskTuple task = taskWithoutResourceUrl();

        //when
        recordHarvestingBolt.execute(anchorTuple, task);

        //then
        verifyErrorEmit();
    }

    @Test
    void harvestingForEmptyRecordId() {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        CommonTaskTuple task = taskWithoutRecordId();

        //when
        recordHarvestingBolt.execute(anchorTuple, task);

        //then
        verifyErrorEmit();
    }

    @Test
    void harvestForEmptyPrefix() {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        CommonTaskTuple task = taskWithoutPrefix();

        //when
        recordHarvestingBolt.execute(anchorTuple, task);

        //then
        verifyErrorEmit();
    }

  private CommonTaskTuple taskWithAllNeededParameters() {
    CommonTaskTuple task = new CommonTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
    task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
    task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
    task.setRecordUri("oaiIdentifier");
    task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, "2020739_Ag_EU_CARARE_2Culture");
    return task;
  }

  private CommonTaskTuple taskWithoutResourceUrl() {
    CommonTaskTuple task = new CommonTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
    task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
    return task;
  }

  private CommonTaskTuple taskWithoutRecordId() {
    CommonTaskTuple task = new CommonTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
    task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
    task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
    task.setMessageProcessingStartTimeInMs(0);
    return task;
  }

  private CommonTaskTuple taskWithoutPrefix() {
    CommonTaskTuple task = new CommonTaskTuple();
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
    task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
    task.setRecordUri("oaiIdentifier");
    task.setMessageProcessingStartTimeInMs(0);
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
