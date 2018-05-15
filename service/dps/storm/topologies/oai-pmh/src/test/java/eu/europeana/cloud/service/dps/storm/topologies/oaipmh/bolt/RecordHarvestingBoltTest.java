package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester.Harvester;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link RecordHarvestingBolt}
 */

@RunWith(MockitoJUnitRunner.class)
public class RecordHarvestingBoltTest {
    @Mock
    private OutputCollector outputCollector;

    @Mock
    private Harvester harvester;

    @InjectMocks
    private RecordHarvestingBolt recordHarvestingBolt = new RecordHarvestingBolt();

    @Test
    public void harvestingForAllParametersSpecified() throws IOException, HarvesterException {
        //given
        when(harvester.harvestRecord(anyString(), anyString(), anyString())).thenReturn(new
                ByteArrayInputStream(new byte[]{}));
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifySuccessfulEmit();
        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
    }

    @Test
    public void shouldEmitErrorOnHarvestingException() throws IOException,
            HarvesterException {
        //given
        when(harvester.harvestRecord(anyString(), anyString(), anyString())).thenThrow(new
                HarvesterException("Some!"));
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifyErrorEmit();
    }

    @Test
    public void harvestingForEmptyUrl() {
        //given
        StormTaskTuple task = taskWithoutResourceUrl();

        //when
        recordHarvestingBolt.execute(task);

        //then
        verifyErrorEmit();
    }

    @Test
    public void harvestingForEmptyRecordId() {
        //given
        StormTaskTuple task = taskWithoutRecordId();

        //when
        recordHarvestingBolt.execute(task);

        //then
        verifyErrorEmit();
    }

    @Test
    public void harvestForEmptyPrefix() {
        //given
        StormTaskTuple task = taskWithoutPrefix();

        //when
        recordHarvestingBolt.execute(task);

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
        return task;
    }

    private StormTaskTuple taskWithoutResourceUrl() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
        task.setSourceDetails(details);
        return task;
    }

    private StormTaskTuple taskWithoutRecordId() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
        task.setSourceDetails(details);
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
        task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
        return task;
    }

    private StormTaskTuple taskWithoutPrefix() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
        task.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, "oaiIdentifier");
        task.setSourceDetails(details);
        return task;
    }

     /**

     * Checks if emit to standard stream occured
     */
    private void verifySuccessfulEmit() {
        verify(outputCollector, times(1)).emit(Mockito.anyList());
        verify(outputCollector, times(0)).emit(eq("NotificationStream"),Mockito.anyList());
    }

    /**
     * Checks if emit to error stream occured
     */
    private void verifyErrorEmit() {

        verify(outputCollector, times(1)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(outputCollector, times(0)).emit(Mockito.anyList());
    }

}
