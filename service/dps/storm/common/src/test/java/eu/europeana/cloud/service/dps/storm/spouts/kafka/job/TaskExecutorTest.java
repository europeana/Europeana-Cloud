package eu.europeana.cloud.service.dps.storm.spouts.kafka.job;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.test.TestHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by Arek on 7/18/2019.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest(TaskExecutor.class)
public class TaskExecutorTest {
    private static final String DATASET_URL = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet";
    private final static String TASK_NAME = "TASK_NAME";
    private final static String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final static String FILE_URL2 = "http://localhost:8080/mcs/records/sourceCloudId2/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName2";
    private final static int QUEUE_MAX_SIZE = 1000;

    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    @Mock
    private TaskStatusChecker taskStatusChecker;

    private TestHelper testHelper;

    private static Date date = new Date();

    private DataSetServiceClient dataSetServiceClient;
    private RecordServiceClient recordServiceClient;
    private FileServiceClient fileServiceClient;
    private RepresentationIterator representationIterator;

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        representationIterator = mock(RepresentationIterator.class);
        doNothing().when(cassandraTaskInfoDAO).updateTask(anyLong(), anyString(), anyString(), any(Date.class));
        doNothing().when(cassandraTaskInfoDAO).dropTask(anyLong(), anyString(), anyString());
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

    private static void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }

    @Test
    public void shouldEmitTheFilesWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);

        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 2);
    }

    @Test
    public void shouldFailWhenReadFileThrowMCSExceptionWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());

        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldFailPerEachFileWhenReadFileThrowDriverExceptionWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());

        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);

        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }


    @Test
    public void shouldEmitTheFilesWhenTaskWithSpecificRevision() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(firstRepresentation));
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(secondRepresentation));

        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL));
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL2));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 2);
    }


    @Test
    public void shouldEmitOnlySampleSizeWhenTaskWithSpecificRevision() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when
        dpsTask.addParameter(PluginParameterKeys.SAMPLE_SIZE, "65");

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList(100);
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);

        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationByRevision(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(Collections.singletonList(firstRepresentation));

        when(fileServiceClient.getFileUri(anyString(), anyString(), anyString(), anyString())).thenReturn(new URI(FILE_URL2));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 65);
    }


    @Test
    public void shouldFailWhenReadFileThrowDriverExceptionWhenSpecificRevisionIsProvided() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(firstRepresentation));
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(secondRepresentation));

        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldFailWhenReadFileThrowMCSExceptionWhenSpecificRevisionIsProvided() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);

        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(firstRepresentation));
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(secondRepresentation));

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }


    @Test
    public void shouldEmitTheFilesWhenTaskWithLatestRevision() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);
        ResultSlice<CloudIdAndTimestampResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdAndTimestampResponseList);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), eq(false), anyString())).thenReturn(resultSlice);


        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(firstRepresentation));
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(secondRepresentation));
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL));
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL2));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 2);
    }


    @Test
    public void shouldEmitOnlySampleSizeWhenTaskWithLatestRevision() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        dpsTask.addParameter(PluginParameterKeys.SAMPLE_SIZE, "65");

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date, 100);
        ResultSlice<CloudIdAndTimestampResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdAndTimestampResponseList);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), eq(false), anyString())).thenReturn(resultSlice);


        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationByRevision(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(Collections.singletonList(firstRepresentation));
        when(fileServiceClient.getFileUri(anyString(), anyString(), anyString(), anyString())).thenReturn(new URI(FILE_URL2));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 65);
    }

    @Test
    public void shouldFailWhenGettingFileThrowMCSExceptionWhenTaskWithLatestRevision() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);
        ResultSlice<CloudIdAndTimestampResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdAndTimestampResponseList);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), eq(false), anyString())).thenReturn(resultSlice);

        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(firstRepresentation));
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(Collections.singletonList(secondRepresentation));

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test(expected = MCSException.class)
    public void shouldReTry3TimesAndFailWhenGettingLatestRevisionThrowMCSException() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        doThrow(MCSException.class).when(dataSetServiceClient).getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean(), anyString());

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.execute();
    }

    @Test(expected = MCSException.class)
    public void shouldReTry3TimesAndFailWhenSpecificRevisionThrowMCSException() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        doThrow(MCSException.class).when(dataSetServiceClient).getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt());

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.execute();
    }

    @Test(expected = DriverException.class)
    public void shouldReTry3TimesAndFailWhenGettingLatestRevisionThrowDriverException() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        doThrow(DriverException.class).when(dataSetServiceClient).getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean(), anyString());

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.execute();
    }

    @Test(expected = DriverException.class)
    public void shouldReTry3TimesAndFailWhenSpecificRevisionThrowDriverException() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        doThrow(DriverException.class).when(dataSetServiceClient).getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt());

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.execute();
    }

    @Test
    public void shouldStopEmittingFilesWhenTaskIsKilled() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false, false, true);
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);

        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 1);
    }

    @Test
    public void shouldNotEmitAnyFilesWhenTaskIsKilledBeforeIteratingRepresentation() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(true);
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);

        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 0);
    }


    @Test
    public void shouldDropTaskInCaseOfException() throws Exception {
        //given
        when(collector.emit(anyListOf(Object.class))).thenReturn(null);
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();

        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);

        doThrow(MCSException.class).when(dataSetServiceClient).getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean(), anyString());

        ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue<>(QUEUE_MAX_SIZE);
        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, anyString(), DATASET_URLS.name(), dpsTask);

        taskExecutor.call();

        //called twice, once per finally inside TaskExecutor.execute() and the other inside call() catch block
        verify(cassandraTaskInfoDAO, times(2)).dropTask(anyLong(), anyString(), anyString());
    }


    private DpsTask getDpsTask() {
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        parametersWithRevision.put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));
        return prepareDpsTask(dataSets, parametersWithRevision);
    }

    private Map<String, String> prepareStormTaskTupleParameters() {
        Map<String, String> parameters = new HashMap<>();
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
