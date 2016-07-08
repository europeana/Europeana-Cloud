package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
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


import static eu.europeana.cloud.service.dps.storm.io.ReadRepresentationBolt.getTestInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ReadRepresentationBoltTest implements TestConstantsHelper {

    private ReadRepresentationBolt instance;
    private OutputCollector oc;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final String FILE_URL2 = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName2";
    private final byte[] FILE_DATA = "Data".getBytes();
    private FileServiceClient fileClient;



    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        fileClient = mock(FileServiceClient.class);
        instance = getTestInstance("URL", oc, fileClient);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        List<Representation> representationList = prepareRepresentationList();
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(representationList));


        when(fileClient.getFileUri(SOURCE + CLOUD_ID,SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(FILE_URL));
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.execute(tuple);
        //then

        String exptectedFileUrls = "{\"FILE_URLS\":[\"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName\"]}";
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertRepresentation(exptectedFileUrls, allValues, 0);
        verifyNoMoreInteractions(oc);
    }

    @Test
    public void successfulExecuteStormTupleWithTwoFiles() throws MCSException, URISyntaxException {
        //given
        List<Representation> representationList = prepareRepresentationListWithTwoFiles();
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(representationList));


        when(fileClient.getFileUri(SOURCE + CLOUD_ID,SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(FILE_URL));
        when(fileClient.getFileUri(SOURCE + CLOUD_ID,SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE + 2)).thenReturn(new URI(FILE_URL2));
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.execute(tuple);
        //then

        String exptectedFileUrls = "{\"FILE_URLS\":[\"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName\"]}";
        String exptectedFileUrls2 = "{\"FILE_URLS\":[\"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName2\"]}";
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(2));
        List<Values> allValues = captor.getAllValues();
        assertRepresentation(exptectedFileUrls, allValues, 0);
        assertRepresentation(exptectedFileUrls2, allValues, 1);
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

    private List<Representation> prepareRepresentationListWithTwoFiles() throws URISyntaxException {
        List<File> files = new ArrayList<>();
        files.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(FILE_URL)));
        files.add(new File("sourceFileName2", "text/plain", "md5", "1", 5, new URI(FILE_URL)));
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER,files,false,new Date());
        List<Representation> representationList = new ArrayList<>();
        representationList.add(representation);
        return representationList;
    }


    private void assertRepresentation(String expectedRepresentation, List<Values> allValues, int index) {
        String representationJson = ((Map<String, String>) allValues.get(index).get(4)).get(PluginParameterKeys.DPS_TASK_INPUT_DATA);
        assertThat(representationJson,is(expectedRepresentation));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters(List<Representation> representation) {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER,"AUTHORIZATION_HEADER");
        String RepresentationsJson = new Gson().toJson(representation);
        parameters.put(PluginParameterKeys.REPRESENTATION, RepresentationsJson);
        return parameters;
    }
}