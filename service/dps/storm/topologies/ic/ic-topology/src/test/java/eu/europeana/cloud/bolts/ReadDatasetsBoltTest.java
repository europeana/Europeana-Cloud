package eu.europeana.cloud.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadDatasetsBolt;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.io.ReadDatasetsBolt.getTestInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;


public class ReadDatasetsBoltTest implements TestConstantsHelper {

    private ReadDatasetsBolt instance;
    private OutputCollector oc;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final byte[] FILE_DATA = "Data".getBytes();


    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        instance = getTestInstance(oc);
    }

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        String dataSetUrls = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet";
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(dataSetUrls));
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        String expectedDataSetUrl = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet";
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertDpsTaskInputData(expectedDataSetUrl, allValues, 0);
        verifyNoMoreInteractions(oc);

    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTupleProcessingDataSets() throws MCSException, URISyntaxException {
        //given
        String dataSetUrls = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet,http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet2";
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(dataSetUrls));
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        String expectedDataSetUrl = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet";
        String expectedDataSetUrl2 = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet2";
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(2));
        List<Values> allValues = captor.getAllValues();
        assertDpsTaskInputData(expectedDataSetUrl, allValues, 0);
        assertDpsTaskInputData(expectedDataSetUrl2, allValues, 1);
        verifyNoMoreInteractions(oc);
    }

    private void assertDpsTaskInputData(String expectedDataSetUrl, List<Values> allValues, int index) {
        String dpsTaskInputData = ((Map<String, String>) allValues.get(index).get(4)).get(PluginParameterKeys.DATASET_URL);
        assertThat(dpsTaskInputData, is(expectedDataSetUrl));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters(String dataSetUrls) {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA, dataSetUrls);
        return parameters;
    }


}