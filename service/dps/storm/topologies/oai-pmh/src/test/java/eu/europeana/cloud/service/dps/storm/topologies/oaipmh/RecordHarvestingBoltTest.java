package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.OAIRequestException;
import com.lyncode.xoai.serviceprovider.parameters.Parameters;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.OAIClientProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * Tests for {@link RecordHarvestingBolt}
 *
 */

@RunWith(MockitoJUnitRunner.class)
public class RecordHarvestingBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "oaiClientProvider")
    private OAIClientProvider oaiClientProvider;

    @Mock
    private OAIClient oaiClient;

    @InjectMocks
    private RecordHarvestingBolt recordHarvestingBolt = new RecordHarvestingBolt();

    @Test
    public void harvestingForAllParametersSpecified() throws OAIRequestException, IOException {

        when(oaiClient.execute(Mockito.any(Parameters.class))).thenReturn(null);
        when(oaiClientProvider.provide(Mockito.anyString())).thenReturn(oaiClient);
        //
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);
        //
        recordHarvestingBolt.execute(spiedTask);

        verifySuccessfulEmit();
        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
    }

    @Test
    public void harvestingForEmptyUrl() throws OAIRequestException {
        StormTaskTuple task = taskWithoutResourceUrl();
        recordHarvestingBolt.execute(task);
        verifyErrorEmit();
    }

    @Test
    public void harvestingForEmptyRecordId(){
        StormTaskTuple task = taskWithoutRecordId();
        recordHarvestingBolt.execute(task);
        verifyErrorEmit();
    }

    @Test
    public void harvestForEmptyPrefix(){
        StormTaskTuple task = taskWithoutPrefix();
        recordHarvestingBolt.execute(task);
        verifyErrorEmit();
    }

    private StormTaskTuple taskWithAllNeededParameters(){
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
        task.setSourceDetails(details);
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA,"urlToOAIEndpoint");
        task.addParameter(PluginParameterKeys.OAI_IDENTIFIER,"oaiIdentifier");
        return task;
    }

    private StormTaskTuple taskWithoutResourceUrl(){
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
        task.setSourceDetails(details);
        return task;
    }

    private StormTaskTuple taskWithoutRecordId(){
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
        task.setSourceDetails(details);
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA,"urlToOAIEndpoint");

        return task;
    }

    private StormTaskTuple taskWithoutPrefix(){
        StormTaskTuple task = new StormTaskTuple();

        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails(null);
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA,"urlToOAIEndpoint");
        task.addParameter(PluginParameterKeys.OAI_IDENTIFIER,"oaiIdentifier");
        task.setSourceDetails(details);
        return task;
    }

    /**
     * Checks if emit to standard stream occured
     */
    private void verifySuccessfulEmit(){
        verify(outputCollector,times(1)).emit(Mockito.any(Tuple.class), Mockito.anyList());
        verify(outputCollector,times(0)).emit(eq("NotificationStream"), Mockito.anyList());
    }

    /**
     * Checks if emit to error stream occured
     */
    private void verifyErrorEmit(){

        verify(outputCollector,times(1)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(outputCollector,times(0)).emit(Mockito.any(Tuple.class), Mockito.anyList());
    }

}
