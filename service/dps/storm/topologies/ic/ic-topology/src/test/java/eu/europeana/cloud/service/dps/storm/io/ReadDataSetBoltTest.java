package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


import static eu.europeana.cloud.service.dps.storm.io.ReadDataSetBolt.getTestInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ReadDataSetBoltTest implements TestConstantsHelper {


    private ReadDataSetBolt instance;
    private OutputCollector oc;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final String FILE_URL2 = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion2/files/sourceFileName2";
    private final byte[] FILE_DATA = "Data".getBytes();
    private DataSetServiceClient datasetClient;



    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        datasetClient = mock(DataSetServiceClient.class);
        instance = getTestInstance("URL", oc, datasetClient);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        String dataSetUrls = "{\"DATASET_URLS\":[\"http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet\"]}";
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(dataSetUrls));
        List<Representation> representationList = prepareRepresentationList();

        when(datasetClient.getDataSetRepresentations("testDataProvider","dataSet")).thenReturn(representationList);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.execute(tuple);
        //then

        Representation expectedRepresentation = representationList.get(0);
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertRepresentation(expectedRepresentation, allValues, 0);
        verifyNoMoreInteractions(oc);
    }



    private List<Representation> prepareRepresentationList() throws URISyntaxException {
        List<File> files = new ArrayList<>();
        files.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(FILE_URL)));
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER,files,false,new Date());
        List<Representation> representationList = new ArrayList<>();
        representationList.add(representation);
        return representationList;
    }

    private List<Representation> prepareRepresentationList2() throws URISyntaxException {
        List<File> files = new ArrayList<>();
        files.add(new File("sourceFileName2", "text/plain", "md5", "1", 5, new URI(FILE_URL2)));
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION + 2, new URI(SOURCE_VERSION_URL2), new URI(SOURCE_VERSION_URL2), DATA_PROVIDER,files,false,new Date());
        List<Representation> representationList = new ArrayList<>();
        representationList.add(representation);
        return representationList;
    }

    private void assertRepresentation(Representation expectedRepresentation, List<Values> allValues, int index) {
        String representationJson = ((Map<String, String>) allValues.get(index).get(4)).get(PluginParameterKeys.REPRESENTATION);
        Type type = new TypeToken<List<Representation>>() {
        }.getType();
        List<Representation> actualRepresentation = new Gson().fromJson(representationJson,type);
        assertThat(actualRepresentation.size(),is(1));
        assertThat(actualRepresentation.get(0).toString(),is(expectedRepresentation.toString()));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters(String dataSetUrls) {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER,"AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA, dataSetUrls);
        return parameters;
    }
}