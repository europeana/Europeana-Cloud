package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link RecordHarvestingBolt}
 */

public class RecordHarvestingBoltTest  {
    @Mock
    private OutputCollector outputCollector;

    @Mock
    private OaiHarvester harvester;

    @InjectMocks
    private RecordHarvestingBolt recordHarvestingBolt = new RecordHarvestingBolt();

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void harvestingForAllParametersSpecified() throws IOException, HarvesterException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);

        InputStream fileContentAsStream = getFileContentAsStream("/sampleEDMRecord.xml");
        when(harvester.harvestRecord(any(), anyString())).thenReturn(fileContentAsStream);
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

        InputStream fileContentAsStream = getFileContentAsStream("/sampleEDMRecord.xml");
        when(harvester.harvestRecord(any(), anyString())).thenReturn(fileContentAsStream);
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(anchorTuple, spiedTask);

        //then
        verifySuccessfulEmit();

        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
        assertTrue(spiedTask.getIdentifiersToUse().contains("http://more.locloud.eu/object/DCU/24927017"));
        assertTrue(spiedTask.getIdentifiersToUse().contains("/2020739_Ag_EU_CARARE_2Cultur/object_DCU_24927017"));
        assertTrue(spiedTask.getIdentifiersToUse().contains("oaiIdentifier"));
        assertTrue(spiedTask.getIdentifiersToUse().size() == 3);
    }

    @Test
    public void shouldEmitErrorOnHarvestingExceptionWhenCannotExctractEuropeanaIdFromEDM() throws HarvesterException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);

        InputStream fileContentAsStream = getFileContentAsStream("/corruptedEDMRecord.xml");
        when(harvester.harvestRecord(any(), anyString())).thenReturn(fileContentAsStream);
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
        task.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, "oaiIdentifier");
        task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
        task.addParameter(PluginParameterKeys.METIS_DATASET_ID, "2020739_Ag_EU_CARARE_2Culture");
        task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
        return task;
    }

    private StormTaskTuple taskWithoutResourceUrl() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
        task.setSourceDetails(details);
        task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
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
        task.setSourceDetails(details);
        task.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
        return task;
    }

    /**
     * Checks if emit to standard stream occured
     */
    private void verifySuccessfulEmit() {
        verify(outputCollector, times(1)).emit(any(Tuple.class), Mockito.anyList());
        verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.anyList());
    }

    /**
     * Checks if emit to error stream occured
     */
    private void verifyErrorEmit() {

        verify(outputCollector, times(1)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
        verify(outputCollector, times(0)).emit(any(Tuple.class), Mockito.anyList());
    }

    private static InputStream getFileContentAsStream(String name) {
        return RecordHarvestingBoltTest.class.getResourceAsStream(name);
    }
}
