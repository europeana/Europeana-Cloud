package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.utils.TaskSpoutInfo;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.test.TestHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by Tarek on 5/21/2018.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest(MCSReaderSpout.class)
public class MCSReaderSpoutTest {
    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;


    @Mock
    private TaskStatusChecker taskStatusChecker;

    @Mock(name = "cache")
    private ConcurrentHashMap<Long, TaskSpoutInfo> cache;

    private TestHelper testHelper;
    private static Date date = new Date();


    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final String FILE_URL2 = "http://localhost:8080/mcs/records/sourceCloudId2/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName2";
    private DataSetServiceClient dataSetServiceClient;
    private RecordServiceClient recordServiceClient;
    private FileServiceClient fileServiceClient;
    private RepresentationIterator representationIterator;


    @InjectMocks
    private MCSReaderSpout mcsReaderSpout = new MCSReaderSpout(null);

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        representationIterator = mock(RepresentationIterator.class);
        doNothing().when(cassandraTaskInfoDAO).updateTask(anyLong(), anyString(), anyString(), any(Date.class));
        TaskSpoutInfo taskSpoutInfo = mock(TaskSpoutInfo.class);
        when(cache.get(anyLong())).thenReturn(taskSpoutInfo);
        doNothing().when(taskSpoutInfo).inc();
        setStaticField(MCSReaderSpout.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
        testHelper = new TestHelper();
        mockMCSClient();
    }

    private void mockMCSClient() throws Exception {
        recordServiceClient = mock(RecordServiceClient.class);
        whenNew(RecordServiceClient.class).withArguments(anyString()).thenReturn(recordServiceClient);
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());

        dataSetServiceClient = mock(DataSetServiceClient.class);
        whenNew(DataSetServiceClient.class).withArguments(anyString()).thenReturn(dataSetServiceClient);
        doNothing().when(dataSetServiceClient).useAuthorizationHeader(anyString());

        fileServiceClient = mock(FileServiceClient.class);
        whenNew(FileServiceClient.class).withArguments(anyString()).thenReturn(fileServiceClient);
        doNothing().when(fileServiceClient).useAuthorizationHeader(anyString());


    }

    static void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }

    @Test
    public void shouldEmitTheFilesWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));
        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);
        verify(collector, times(2)).emit(anyListOf(Object.class));
        verify(collector, times(0)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldTry10TimesAndFailPerEachFileWhenReadFileThrowMCSExceptionWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);
        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldTry10TimesAndFailPerEachFileWhenReadFileThrowDriverExceptionWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);

        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);
        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }


    @Test
    public void shouldEmitTheFilesWhenTaskWithSpecificRevision() throws MCSException, URISyntaxException {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        parametersWithRevision.put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);


        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        when(dataSetServiceClient.getDataSetRevisions(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(cloudIdCloudTagsResponses);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);

        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);

        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL));
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL2));

        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);

        verify(collector, times(2)).emit(anyListOf(Object.class));
        verify(collector, times(0)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }


    @Test
    public void shouldTry10TimesAndFailWhenReadFileThrowDriverExceptionWhenSpecificRevisionIsProvided() throws MCSException, URISyntaxException {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        parametersWithRevision.put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);


        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        when(dataSetServiceClient.getDataSetRevisions(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(cloudIdCloudTagsResponses);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);

        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);

        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldTry10TimesAndFailWhenReadFileThrowMCSExceptionWhenSpecificRevisionIsProvided() throws MCSException, URISyntaxException {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        parametersWithRevision.put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);


        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        when(dataSetServiceClient.getDataSetRevisions(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(cloudIdCloudTagsResponses);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);

        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }


    @Test
    public void shouldEmitTheFilesWhenTaskWithLatestRevision() throws MCSException, URISyntaxException {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);


        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);

        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);

        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL));
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL2));

        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);

        verify(collector, times(2)).emit(anyListOf(Object.class));
        verify(collector, times(0)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }

    @Test
    public void shouldTry10TimesAndFailPerEachFileWhenGettingFileThrowMCSEXceptionWhenTaskWithLatestRevision() throws MCSException, URISyntaxException {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);


        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);

        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(11)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }

    @Test
    public void shouldStopEmittingFilesWhenTaskIsKilled() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false,false,true);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));
        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);
        verify(collector, times(1)).emit(anyListOf(Object.class));
        verify(collector, times(0)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldNotEmitAnyFilesWhenTaskIsKilledBeforeIteratingRepresentation() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(true);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add("http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet");
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));
        mcsReaderSpout.execute(DATASET_URLS.name(), dpsTask);
        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(collector, times(0)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }



    private HashMap<String, String> prepareStormTaskTupleParameters() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        return parameters;
    }

    private DpsTask prepareDpsTask(List<String> dataSetUrls, Map<String, String> parameters) {
        DpsTask dpsTask = new DpsTask();
        Map<InputDataType, List<String>> inputData = new HashMap<>();
        inputData.put(DATASET_URLS, dataSetUrls);
        dpsTask.setInputData(inputData);

        dpsTask.setParameters(parameters);

        dpsTask.setTaskName(TASK_NAME);
        dpsTask.setOutputRevision(new Revision());
        return dpsTask;

    }

    private Map<String, String> prepareStormTaskTupleParametersForRevision() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.REVISION_NAME, REVISION_NAME);
        parameters.put(PluginParameterKeys.REVISION_PROVIDER, REVISION_PROVIDER);
        parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        return parameters;
    }

}