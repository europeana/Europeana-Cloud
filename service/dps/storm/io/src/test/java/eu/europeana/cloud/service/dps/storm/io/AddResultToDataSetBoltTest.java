package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URISyntaxException;

import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AddResultToDataSetBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;
    @Mock(name = "dataSetClient")
    private DataSetServiceClient dataSetServiceClient;

    @InjectMocks
    private AddResultToDataSetBolt addResultToDataSetBolt;


    @Before
    public void init() throws IllegalAccessException, MCSException, URISyntaxException {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    }


    private StormTaskTuple stormTaskTuple;
    private static final String DATASET_URL = "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/s1";
    private static final String DATASET_URL2 = "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/s2";
    private static final String FILE_URL = "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/NEW_REPRESENTATION_NAME/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2";

    public final void verifyMethodExecutionNumber(int expectedAssignRepresentationToDataCallTimes, int expectedEmitCallTimes) throws MCSException {
        when(outputCollector.emit(anyString(), anyList())).thenReturn(null);
        addResultToDataSetBolt.addRepresentationToDataSets(stormTaskTuple, dataSetServiceClient);
        verify(dataSetServiceClient, times(expectedAssignRepresentationToDataCallTimes)).assignRepresentationToDataSet(anyString(), anyString(), anyString(), anyString(), anyString());
        verify(outputCollector, times(expectedEmitCallTimes)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }


    @Test
    public void shouldEmmitNotificationWhenDataSetListHasOneElement() throws MCSException, URISyntaxException {
        //given
        stormTaskTuple = prepareTupleWithSingleDataSet();
        verifyMethodExecutionNumber(1, 1);
    }

    @Test
    public void shouldEmitInfoWhenDataSetListIsEmpty() throws MCSException {
        stormTaskTuple = prepareTupleWithEmptyDataSetList();
        verifyMethodExecutionNumber(0, 1);
    }


    @Test
    public void shouldEmmitInfoWhenDataSetListHasMoreThanOneElement() throws MCSException {
        stormTaskTuple = prepareTupleWithMultipleDataSets();
        verifyMethodExecutionNumber(2, 1);
    }

    @Test
    public void shouldEmmitNotificationWhenOutputUrlIsEmpty() throws MCSException {
        stormTaskTuple = prepareTupleWithEmptyOutputUrl();
        verifyMethodExecutionNumber(0, 1);
    }

    @Test
    public void shouldEmmitNotificationWrongDatasetUrl() throws MCSException {
        stormTaskTuple = prepareTupleWithWrongDatasetUrl();
        verifyMethodExecutionNumber(0, 1);
    }

    @Test
    public void shouldRetry10TimesBeforeFailingWhenThrowingMCSException() throws MCSException {
        stormTaskTuple = prepareTupleWithSingleDataSet();
        doThrow(MCSException.class).when(dataSetServiceClient).assignRepresentationToDataSet(anyString(), anyString(), anyString(), anyString(), anyString());
        verifyMethodExecutionNumber(11, 1);
    }
    @Test
    public void shouldRetry10TimesBeforeFailingWhenThrowingDriverException() throws MCSException {
        stormTaskTuple = prepareTupleWithSingleDataSet();
        doThrow(DriverException.class).when(dataSetServiceClient).assignRepresentationToDataSet(anyString(), anyString(), anyString(), anyString(), anyString());
        verifyMethodExecutionNumber(11, 1);
    }


    private StormTaskTuple prepareTupleWithEmptyOutputUrl() {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
        return tuple;
    }

    private StormTaskTuple prepareTupleWithEmptyDataSetList() {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, FILE_URL);
        return tuple;
    }

    private StormTaskTuple prepareTupleWithSingleDataSet() {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, FILE_URL);
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
        return tuple;
    }

    private StormTaskTuple prepareTupleWithMultipleDataSets() {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, FILE_URL);
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL + "," + DATASET_URL2);
        return tuple;
    }

    private StormTaskTuple prepareTupleWithWrongDatasetUrl() {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, FILE_URL);
        tuple.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "sample_sample_sample");
        return tuple;
    }

}