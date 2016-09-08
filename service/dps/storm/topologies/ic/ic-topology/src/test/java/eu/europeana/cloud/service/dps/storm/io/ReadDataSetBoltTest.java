package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import sun.misc.Version;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt.getTestInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ReadDataSetBoltTest implements TestConstantsHelper {


    private ReadDatasetBolt instance;
    private OutputCollector oc;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final byte[] FILE_DATA = "Data".getBytes();
    private DataSetServiceClient datasetClient;
    private RepresentationIterator representationIterator;


    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        datasetClient = mock(DataSetServiceClient.class);
        representationIterator = mock(RepresentationIterator.class);
        instance = getTestInstance("http://localhost:8080/mcs", oc);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        String dataSetUrl = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet";
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(dataSetUrl));
        Representation representation = prepareRepresentation();
        when(datasetClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.emitSingleRepresentationFromDataSet(tuple, datasetClient);
        //then
        Representation expectedRepresentation = representation;
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertRepresentation(expectedRepresentation, allValues);
        verifyNoMoreInteractions(oc);
    }


    private Representation prepareRepresentation() throws URISyntaxException

    {
        List<File> files = new ArrayList<>();
        List<Revision>  revisions = new ArrayList<>();
        files.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(FILE_URL)));
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER, files,revisions, false, new Date());
        return representation;
    }

    private void assertRepresentation(Representation expectedRepresentation, List<Values> allValues) {
        String representationJson = ((Map<String, String>) allValues.get(0).get(4)).get(PluginParameterKeys.REPRESENTATION);
        Type type = new TypeToken<Representation>() {
        }.getType();
        Representation actualRepresentation = new Gson().fromJson(representationJson, type);

        assertThat(actualRepresentation, is(expectedRepresentation));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters(String dataSetUrl) {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.DATASET_URL, dataSetUrl);
        return parameters;
    }
}