package eu.europeana.cloud.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


import static eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt.getTestInstance;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;


public class ReadDatasetBoltTest implements TestConstantsHelper {

    private ReadDatasetBolt instance;
    private OutputCollector oc;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final byte[] FILE_DATA = "Data".getBytes();
    private DataSetServiceClient datasetClient;
    private FileServiceClient fileClient;


    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        datasetClient = mock(DataSetServiceClient.class);
        fileClient = mock(FileServiceClient.class);
        instance = getTestInstance("URL", oc, datasetClient, fileClient);

    }

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters());
        List<Representation> representationList = prepareRepresentationList();


        when(datasetClient.getDataSetRepresentations("testDataProvider","dataSet")).thenReturn(representationList);
        when(fileClient.getFileUri(SOURCE + CLOUD_ID,SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(FILE_URL));
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.execute(tuple);
        //then
        verify(oc,times(1)).emit(any(Tuple.class),anyList());

    }

    private List<Representation> prepareRepresentationList() throws URISyntaxException {
        List<File> files = new ArrayList<>();
        files.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(FILE_URL)));
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL2), new URI(SOURCE_VERSION_URL2), DATA_PROVIDER,files,false,new Date());
        List<Representation> representationList = new ArrayList<>();
        representationList.add(representation);
        return representationList;
    }

    private HashMap<String, String> prepareStormTaskTupleParameters() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER,"AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA,"{\"DATASET_URLS\":[\"http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet\"]}");
        return parameters;
    }

}