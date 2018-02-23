package eu.europeana.cloud.service.dps.storm.io;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.test.TestHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;

@RunWith(MockitoJUnitRunner.class)
public class ReadDataSetBoltTest {


    @Mock(name = "outputCollector")
    private OutputCollector oc;


    @Mock
    private CassandraTaskInfoDAO taskInfoDAO;


    @InjectMocks
    private ReadDatasetBolt instance = new ReadDatasetBolt("http://localhost:8080/mcs");


    private final int TASK_ID = 1;
    private final byte[] FILE_DATA = "Data".getBytes();
    private DataSetServiceClient datasetClient;
    private RepresentationIterator representationIterator;
    private RecordServiceClient recordServiceClient;
    private static Date date = new Date();
    private TestHelper testHelper;


    @Before
    public void init() {
        datasetClient = mock(DataSetServiceClient.class);
        recordServiceClient = mock(RecordServiceClient.class);
        representationIterator = mock(RepresentationIterator.class);
        testHelper = new TestHelper();

    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(SOURCE_DATASET_URL), new Revision());
        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>();
        representations.add(representation);
        when(datasetClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        assertBoltExecutionResults(tuple, representations);
    }


    @Test
    public void killTaskShouldPreventEmittingRepresentation() throws MCSException, URISyntaxException {
        //given
        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false, true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(SOURCE_DATASET_URL), new Revision());
        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>();
        representations.add(representation);
        when(datasetClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, true, false);
        when(representationIterator.next()).thenReturn(representation);
        assertBoltExecutionResults(tuple, representations);
    }


    @Test
    public void successfulStormTupleExecutionWithLatestRevisions() throws MCSException, URISyntaxException {
        //given
        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParametersForRevision(SOURCE_DATASET_URL), new Revision());
        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>();
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);


        when(datasetClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);

        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);
        assertBoltExecutionResults(tuple, representations);
    }

    @Test
    public void killTaskShouldPreventEmittingRepresentationWhenExecutionWithLatestRevisions() throws MCSException, URISyntaxException {
        //given
        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false, true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParametersForRevision(SOURCE_DATASET_URL), new Revision());
        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>();
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);
        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);

        when(datasetClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);

        assertBoltExecutionResults(tuple, representations.subList(0, 1));
    }

    private void assertBoltExecutionResults(StormTaskTuple tuple, List<Representation> representations) {

        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.emitSingleRepresentationFromDataSet(tuple, datasetClient, recordServiceClient);
        //then
        int expectedExecutionTimes = representations.size();
        verify(oc, times(expectedExecutionTimes)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(expectedExecutionTimes));
        List<Values> allValues = captor.getAllValues();
        for (int i = 0; i < expectedExecutionTimes; i++) {
            assertEquals(allValues.get(i).size(), 7);
            assertTrue(allValues.get(i).get(4) instanceof Map);
            assertRepresentation(representations.get(i), ((Map<String, String>) allValues.get(i).get(4)).get(PluginParameterKeys.REPRESENTATION));
        }

        verifyNoMoreInteractions(oc);
    }


    @Test
    public void successfulStormTupleExecutionWithSpecificRevisions() throws MCSException, URISyntaxException {
        //given
        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParametersForRevision(SOURCE_DATASET_URL), new Revision());
        tuple.getParameters().put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>();
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        when(datasetClient.getDataSetRevisions(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(cloudIdCloudTagsResponses);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);

        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);
        assertBoltExecutionResults(tuple, representations);
    }

    @Test
    public void killTaskShouldPreventEmittingRepresentationWhenExecutionWithSpecificRevisions() throws MCSException, URISyntaxException {
        //given
        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false, true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParametersForRevision(SOURCE_DATASET_URL), new Revision());
        tuple.getParameters().put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>();
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        when(datasetClient.getDataSetRevisions(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(cloudIdCloudTagsResponses);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);

        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);

        assertBoltExecutionResults(tuple, representations.subList(0, 1));
    }

    private void assertRepresentation(Representation expectedRepresentation, String representationJSON) {
        Type type = new TypeToken<Representation>() {
        }.getType();
        Representation actualRepresentation = new Gson().fromJson(representationJSON, type);

        assertThat(actualRepresentation, is(expectedRepresentation));

    }

    private HashMap<String, String> prepareStormTaskTupleParameters(String dataSetUrl) {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        parameters.put(PluginParameterKeys.DATASET_URL, dataSetUrl);
        return parameters;
    }

    private HashMap<String, String> prepareStormTaskTupleParametersForRevision(String dataSetUrl) {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.DATASET_URL, dataSetUrl);
        parameters.put(PluginParameterKeys.REVISION_NAME, REVISION_NAME);
        parameters.put(PluginParameterKeys.REVISION_PROVIDER, REVISION_PROVIDER);
        parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        return parameters;
    }

}