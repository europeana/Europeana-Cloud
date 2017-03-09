package eu.europeana.cloud.service.dps.storm.io;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

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
    private final byte[] FILE_DATA = "Data".getBytes();
    private DataSetServiceClient datasetClient;
    private RepresentationIterator representationIterator;
    private RecordServiceClient recordServiceClient;
    private static Date date = new Date();


    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        datasetClient = mock(DataSetServiceClient.class);
        recordServiceClient = mock(RecordServiceClient.class);
        representationIterator = mock(RepresentationIterator.class);
        instance = getTestInstance("http://localhost:8080/mcs", oc);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(SOURCE_DATASET_URL));
        Representation representation = prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        when(datasetClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.emitSingleRepresentationFromDataSet(tuple, datasetClient, recordServiceClient);
        //then
        Representation expectedRepresentation = representation;
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertRepresentation(expectedRepresentation, ((Map<String, String>) allValues.get(0).get(4)).get(PluginParameterKeys.REPRESENTATION));
        verifyNoMoreInteractions(oc);
    }


    @Test
    public void successfulStormTupleExecutionWithLatestRevisions() throws MCSException, URISyntaxException {
        //given
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParametersForRevision(SOURCE_DATASET_URL));
        Representation firstRepresentation = prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = prepareCloudIdAndTimestampResponseList();

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);


        when(datasetClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);

        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.emitSingleRepresentationFromDataSet(tuple, datasetClient, recordServiceClient);
        //then
        Representation expectedFirstRepresentation = firstRepresentation;
        Representation expectedSecondRepresentation = secondRepresentation;
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(2));
        List<Values> allValues = captor.getAllValues();
        assertRepresentation(expectedFirstRepresentation, ((Map<String, String>) allValues.get(0).get(4)).get(PluginParameterKeys.REPRESENTATION));
        assertRepresentation(expectedSecondRepresentation, ((Map<String, String>) allValues.get(1).get(4)).get(PluginParameterKeys.REPRESENTATION));
        verifyNoMoreInteractions(oc);
    }


    @Test
    public void successfulStormTupleExecutionWithSpecificRevisions() throws MCSException, URISyntaxException {
        //given
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParametersForRevision(SOURCE_DATASET_URL));
        tuple.getParameters().put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));

        Representation firstRepresentation = prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = prepareCloudTagsResponsesList();
        when(datasetClient.getDataSetRevisions(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(cloudIdCloudTagsResponses);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);


        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        //when
        instance.emitSingleRepresentationFromDataSet(tuple, datasetClient, recordServiceClient);
        //then
        Representation expectedFirstRepresentation = firstRepresentation;
        Representation expectedSecondRepresentation = secondRepresentation;

        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());

        assertThat(captor.getAllValues().size(), is(2));
        List<Values> allValues = captor.getAllValues();
        assertRepresentation(expectedFirstRepresentation, ((Map<String, String>) allValues.get(0).get(4)).get(PluginParameterKeys.REPRESENTATION));
        assertRepresentation(expectedSecondRepresentation, ((Map<String, String>) allValues.get(1).get(4)).get(PluginParameterKeys.REPRESENTATION));
        verifyNoMoreInteractions(oc);
    }


    private Representation prepareRepresentation(String cloudId, String representationName, String version, String fileUrl,
                                                 String dataProvider, boolean persistent, Date creationDate) throws URISyntaxException

    {
        List<File> files = new ArrayList<>();
        List<Revision> revisions = new ArrayList<>();
        files.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(fileUrl)));
        Representation representation = new Representation(cloudId, representationName, version, new URI(fileUrl), new URI(fileUrl), dataProvider, files, revisions, persistent, creationDate);
        return representation;
    }

    private List<CloudIdAndTimestampResponse> prepareCloudIdAndTimestampResponseList() {
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = new ArrayList<>();
        CloudIdAndTimestampResponse cloudIdAndTimestampResponse = new CloudIdAndTimestampResponse(SOURCE + CLOUD_ID, date);
        CloudIdAndTimestampResponse cloudIdAndTimestampResponse2 = new CloudIdAndTimestampResponse(SOURCE + CLOUD_ID2, date);
        cloudIdAndTimestampResponseList.add(cloudIdAndTimestampResponse);
        cloudIdAndTimestampResponseList.add(cloudIdAndTimestampResponse2);
        return cloudIdAndTimestampResponseList;
    }


    private List<CloudTagsResponse> prepareCloudTagsResponsesList() {
        List<CloudTagsResponse> CloudTagsResponseList = new ArrayList<>();
        CloudTagsResponse cloudTagsResponseResponse1 = new CloudTagsResponse(SOURCE + CLOUD_ID, true, false, false);
        CloudTagsResponse cloudTagsResponseResponse12 = new CloudTagsResponse(SOURCE + CLOUD_ID2, true, false, false);
        CloudTagsResponseList.add(cloudTagsResponseResponse1);
        CloudTagsResponseList.add(cloudTagsResponseResponse12);
        return CloudTagsResponseList;
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