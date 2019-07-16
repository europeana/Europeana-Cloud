package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.TaskState;
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
import eu.europeana.cloud.service.dps.storm.spouts.kafka.job.TaskExecutor;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.test.TestHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.junit.Assert.*;
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
    public static final String DATASET_URL = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet";
    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    @Mock
    private TaskStatusChecker taskStatusChecker;

    private static final boolean OLD_FLAG = false;


    private TestHelper testHelper;
    private static Date date = new Date();


    private final static String TASK_NAME = "TASK_NAME";
    private final static String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final static String FILE_URL2 = "http://localhost:8080/mcs/records/sourceCloudId2/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName2";
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
        doNothing().when(cassandraTaskInfoDAO).dropTask(anyLong(), anyString(), anyString());
        setStaticField(MCSReaderSpout.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
        testHelper = new TestHelper();
        mcsReaderSpout.taskDownloader.taskQueue.clear();
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
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        assertEquals(mcsReaderSpout.taskDownloader.tuplesWithFileUrls.size(), 2);
    }

    @Test
    public void shouldFailWhenReadFileThrowMCSExceptionWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldFailPerEachFileWhenReadFileThrowDriverExceptionWhenNoRevisionIsSpecified() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);

        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }


    @Test
    public void shouldEmitTheFilesWhenTaskWithSpecificRevision() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);


        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentation);

        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL));
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL2));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        assertEquals(mcsReaderSpout.taskDownloader.tuplesWithFileUrls.size(), 2);
    }


    @Test
    public void shouldEmitOnlySampleSizeWhenTaskWithSpecificRevision() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when
        dpsTask.addParameter(PluginParameterKeys.SAMPLE_SIZE, "65");

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(firstRepresentation);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList(100);
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);

        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationByRevision(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(firstRepresentation);

        when(fileServiceClient.getFileUri(anyString(), anyString(), anyString(), anyString())).thenReturn(new URI(FILE_URL2));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        assertEquals(mcsReaderSpout.taskDownloader.tuplesWithFileUrls.size(), 65);
    }


    @Test
    public void shouldFailWhenReadFileThrowDriverExceptionWhenSpecificRevisionIsProvided() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);


        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentation);

        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(DriverException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldFailWhenReadFileThrowMCSExceptionWhenSpecificRevisionIsProvided() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL_FILE2, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);


        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt())).thenReturn(resultSlice);

        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentation);

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }


    @Test
    public void shouldEmitTheFilesWhenTaskWithLatestRevision() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);
        ResultSlice<CloudIdAndTimestampResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdAndTimestampResponseList);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), eq(false), anyString())).thenReturn(resultSlice);


        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL));
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL2));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        assertEquals(mcsReaderSpout.taskDownloader.tuplesWithFileUrls.size(), 2);
    }


    @Test
    public void shouldEmitOnlySampleSizeWhenTaskWithLatestRevision() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        dpsTask.addParameter(PluginParameterKeys.SAMPLE_SIZE, "65");

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(firstRepresentation);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date, 100);
        ResultSlice<CloudIdAndTimestampResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdAndTimestampResponseList);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), eq(false), anyString())).thenReturn(resultSlice);


        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationByRevision(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(firstRepresentation);
        when(fileServiceClient.getFileUri(anyString(), anyString(), anyString(), anyString())).thenReturn(new URI(FILE_URL2));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        assertEquals(mcsReaderSpout.taskDownloader.tuplesWithFileUrls.size(), 65);
    }

    @Test
    public void shouldFailWhenGettingFileThrowMCSExceptionWhenTaskWithLatestRevision() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        List<Representation> representations = new ArrayList<>(2);
        representations.add(firstRepresentation);
        representations.add(secondRepresentation);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);
        ResultSlice<CloudIdAndTimestampResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdAndTimestampResponseList);
        resultSlice.setNextSlice(null);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), eq(false), anyString())).thenReturn(resultSlice);

        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentationByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentation);

        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        verify(collector, times(0)).emit(anyListOf(Object.class));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(fileServiceClient, times(1)).getFileUri(eq(SOURCE + CLOUD_ID2), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        verify(collector, times(2)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Test
    public void shouldReTry3TimesAndFailWhenGettingLatestRevisionThrowMCSException() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        doThrow(MCSException.class).when(dataSetServiceClient).getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean(), anyString());

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
            executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                    mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                    mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();
        } catch(ExecutionException e) {
            assertTrue(e.getCause() instanceof  MCSException);
        }
    }

    @Test
    public void shouldReTry3TimesAndFailWhenSpecificRevisionThrowMCSException() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        doThrow(MCSException.class).when(dataSetServiceClient).getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt());

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
            executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                    mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                    mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();
        } catch(ExecutionException e) {
            assertTrue(e.getCause() instanceof  MCSException);
        }
    }

    @Test
    public void shouldReTry3TimesAndFailWhenGettingLatestRevisionThrowDriverException() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        DpsTask dpsTask = prepareDpsTask(dataSets, parametersWithRevision);
        doThrow(DriverException.class).when(dataSetServiceClient).getLatestDataSetCloudIdByRepresentationAndRevisionChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean(), anyString());

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
            executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                    mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                    mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();
        } catch(ExecutionException e) {
            assertTrue(e.getCause() instanceof  DriverException);
        }
    }

    @Test
    public void shouldReTry3TimesAndFailWhenSpecificRevisionThrowDriverException() throws Exception {
        //given
        when(collector.emit(anyList())).thenReturn(null);
        //given
        DpsTask dpsTask = getDpsTask();
        //when

        doThrow(DriverException.class).when(dataSetServiceClient).getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyInt());

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
            executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                    mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                    mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();
        } catch(ExecutionException e) {
            assertTrue(e.getCause() instanceof  DriverException);
        }
    }

    private DpsTask getDpsTask() {
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        Map<String, String> parametersWithRevision = prepareStormTaskTupleParametersForRevision();
        parametersWithRevision.put(PluginParameterKeys.REVISION_TIMESTAMP, DateHelper.getUTCDateString(date));
        return prepareDpsTask(dataSets, parametersWithRevision);
    }


    @Test
    public void shouldStopEmittingFilesWhenTaskIsKilled() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false, false, true);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        assertEquals(mcsReaderSpout.taskDownloader.tuplesWithFileUrls.size(), 1);
    }

    @Test
    public void shouldNotEmitAnyFilesWhenTaskIsKilledBeforeIteratingRepresentation() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(true);
        when(collector.emit(anyList())).thenReturn(null);
        //given
        List<String> dataSets = new ArrayList<>();
        dataSets.add(DATASET_URL);
        DpsTask dpsTask = prepareDpsTask(dataSets, prepareStormTaskTupleParameters());
        //when

        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);
        when(dataSetServiceClient.getRepresentationIterator(eq("testDataProvider"), eq("dataSet"))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));

        ExecutorService executorService = Executors.newFixedThreadPool(MCSReaderSpout.INTERNAL_THREADS_NUMBER);
        executorService.submit(new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                mcsReaderSpout.taskDownloader.tuplesWithFileUrls, dataSetServiceClient, recordServiceClient, fileServiceClient,
                mcsReaderSpout.mcsClientURL, DATASET_URLS.name(), dpsTask)).get();

        assertEquals(mcsReaderSpout.taskDownloader.tuplesWithFileUrls.size(), 0);
    }

    @Test
    public void deactivateShouldClearTheTaskQueue() throws Exception {
        final int taskCount = 10;
        for (int i = 0; i < taskCount; i++) {
            mcsReaderSpout.taskDownloader.taskQueue.put(new DpsTask());
        }
        assertTrue(!mcsReaderSpout.taskDownloader.taskQueue.isEmpty());
        mcsReaderSpout.deactivate();
        assertTrue(mcsReaderSpout.taskDownloader.taskQueue.isEmpty());
        verify(cassandraTaskInfoDAO, atLeast(taskCount)).dropTask(anyLong(), anyString(), eq(TaskState.DROPPED.toString()));
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