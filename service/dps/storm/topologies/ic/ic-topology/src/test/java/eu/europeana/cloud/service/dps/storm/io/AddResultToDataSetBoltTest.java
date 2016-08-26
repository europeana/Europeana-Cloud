package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URISyntaxException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 
 */
@RunWith(PowerMockRunner.class)
public class AddResultToDataSetBoltTest implements TestConstantsHelper{

    private OutputCollector oc;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final byte[] FILE_DATA = "Data".getBytes();
    private StormTaskTuple stormTaskTuple;

    @Mock(name = "dataSetClient")
    private DataSetServiceClient datasetClient;

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;
    
    @InjectMocks
    private AddResultToDataSetBolt bolt;

    @Before
    public void init() throws IllegalAccessException, MCSException, URISyntaxException {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    }

    @Test
    public void shouldEmmitPropperNotificationWhenOutputUrlIsEmpty(){
        stormTaskTuple = prepareTupleWithEmptyOutputUrl();
        bolt.execute(stormTaskTuple);
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
        verify(outputCollector, times(1)).emit(any(Tuple.class), anyListOf(Object.class));
    }

    @Test
    public void shouldEmitProperInfoWhenDataSetListIsEmpty() {
        stormTaskTuple = prepareTupleWithEmptyDataSetList();
        bolt.execute(stormTaskTuple);
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
        verify(outputCollector, times(1)).emit(any(Tuple.class), anyListOf(Object.class));
    }

    @Test
    public void shouldEmmitPropperInfoWhenDataSetListHasOneElement() throws MCSException {
        stormTaskTuple = prepareTupleWithSingleDataSet();
        bolt.execute(stormTaskTuple);
        verify(datasetClient,times(1)).assignRepresentationToDataSet(anyString(),anyString(),anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(any(Tuple.class), anyListOf(Object.class));
    }

    @Test
    public void shouldEmmitPropperInfoWhenDataSetListHasMoreThanOneElement() throws MCSException {
        stormTaskTuple = prepareTupleWithMoreDataSets();
        bolt.execute(stormTaskTuple);
        verify(datasetClient,atLeast(2)).assignRepresentationToDataSet(anyString(),anyString(),anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(any(Tuple.class), anyListOf(Object.class));
    }
    
    @Test
    public void shouldEmmitPropperInfoWhenDataSetListIsNotParseable() throws MCSException {
        stormTaskTuple = prepareTupleWithWrongDatasetUrl();
        bolt.execute(stormTaskTuple);
        verify(datasetClient,times(0)).assignRepresentationToDataSet(anyString(),anyString(),anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {

        stormTaskTuple = prepareTupleWithMoreDataSets();
        bolt.execute(stormTaskTuple);
        verify(datasetClient, times(2)).assignRepresentationToDataSet(anyString(), anyString(), anyString(), anyString(), anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
        verify(outputCollector, times(1)).emit(any(Tuple.class), anyListOf(Object.class));
    }
    
    
    private StormTaskTuple prepareTupleWithEmptyOutputUrl(){
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/s2");
        return tuple;
    }
    
    private StormTaskTuple prepareTupleWithEmptyDataSetList(){
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/NEW_REPRESENTATION_NAME/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2");
        return tuple;
    }
    
    private StormTaskTuple prepareTupleWithSingleDataSet(){
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/NEW_REPRESENTATION_NAME/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2");
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/s2");
        return tuple;
    }
    
    private StormTaskTuple prepareTupleWithMoreDataSets(){
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/NEW_REPRESENTATION_NAME/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2");
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/s2,http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/s3");
        return tuple;
    }
    
    private StormTaskTuple prepareTupleWithWrongDatasetUrl(){
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/NEW_REPRESENTATION_NAME/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2");
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "sample_sample_sample");
        return tuple;
    }
    

    private StormTaskTuple prepareBasicTaskTuple(){
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/s2");
        return tuple;
    }
}
