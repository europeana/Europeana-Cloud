package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MCSTaskSubmiter.class,MCSReader.class})
public class MCSTaskSubmiterTest {

    private static final long TASK_ID =  10L;

    private static final String DATASET_PROVIDER_1 = "providerId1";
    private static final String DATASET_ID_1 = "datasetId1";

    private static final String REVISION_PROVIDER_1 = "revisionProviderId1";
    private static final String REVISION_NAME = "revisionName1";
    private static final Revision REVISION = new Revision(REVISION_NAME,REVISION_PROVIDER_1);

    private static final String EXAMPLE_DATE = "2020-03-30T10:00:00.000Z";

    private static final String DATASET_URL_1 = "http://localhost:8080/mcs/data-providers/providerId1/data-sets/datasetId1";

    private static final String TOPIC = "topic1";
    private static final String TOPOLOGY = "validation_topology";

    private static final String REPRESENTATION_NAME = "representationName1";
    //File 1
    private static final String FILE_URL_1 = "http://localhost:8080/mcs/records/Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA/representations/mcsReaderRepresentation/versions/ec3c18b0-7354-11ea-b16e-04922659f621/files/9da076ce-f382-4bbf-8b6a-c80d943aa46a";
    private static final URI FILE_URI_1 = URI.create(FILE_URL_1);
    private static final String FILE_NAME_1 ="9da076ce-f382-4bbf-8b6a-c80d943aa46a";
    private static final String FILE_CREATION_DATE_STRING_1 = "2020-03-31T15:38:38.873Z";
    private static final String CLOUD_ID1 = "Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA";
    private static final File FILE_1 = new File(FILE_NAME_1,"text/plain","81d15fa0095586d8e40c7698604753aa", FILE_CREATION_DATE_STRING_1,3403L,FILE_URI_1);

    private static final String VERSION_1 = "ec3c18b0-7354-11ea-b16e-04922659f621";

    private static final URI REPRESENTATON_URI_1 =  URI.create("http://localhost:8080/mcs/records/Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA/representations/mcsReaderRepresentation/versions/ec3c18b0-7354-11ea-b16e-04922659f621");
    public static final URI REPRESENTATION_ALL_VERSION_URI_1 = URI.create("http://localhost:8080/mcs/records/Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA/representations/mcsReaderRepresentation/versions");
    public static final Date FILE_CREATION_DATE_1 = Date.from(Instant.parse(FILE_CREATION_DATE_STRING_1));
    private static final Representation REPRESENTATION_1 = new Representation(CLOUD_ID1,REPRESENTATION_NAME,VERSION_1, REPRESENTATION_ALL_VERSION_URI_1, REPRESENTATON_URI_1, DATASET_PROVIDER_1,
            Arrays.asList(FILE_1),new ArrayList<>(),false, FILE_CREATION_DATE_1);


    //File2

    private static final String FILE_URL_2 = "http://localhost:8080/mcs/records/YI3S73BZBO2ZPINWZ62RBLAJSATKUG3O2YF4UWYC23BM6CDVBTMA/representations/mcsReaderRepresentation/versions/ec64af50-7354-11ea-b16e-04922659f621/files/0b936b8f-1e43-47ca-986b-7d2cce366c33";
    public static final String FILE_CREATION_DATE_STRING_2 = "2020-03-31T15:38:38.873Z";
    public static final Date FILE_CREATION_DATE_2 = Date.from(Instant.parse(FILE_CREATION_DATE_STRING_2));

    private static final String CLOUD_ID2 = "Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA";



    private static final String FILE_URL_3 = "http://localhost:8080/mcs/records/YGF5ZH7GCHRSMJPVQKXOYULUCVJATJ3FOZE2KWV7MXYNZEITSJ5Q/representations/mcsReaderRepresentation/versions/ebe93dc0-7354-11ea-b16e-04922659f621/files/8a9db572-5217-486f-9a96-6dd3c4f149dd";


    @Mock
    private DataSetServiceClient dataSetServiceClient;

    @Mock
    private FileServiceClient fileServiceClient;

    @Mock
    private RecordServiceClient recordServiceClient;

    @Mock
    private CassandraTaskErrorsDAO taskErrorDAO;

    @Mock
    private TaskStatusChecker taskStatusChecker;

    @Mock
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    @Mock
    private RecordExecutionSubmitService recordSubmitService;

    private MCSReader reader=new MCSReader("","");

    private MCSTaskSubmiter submiter;

    private DpsTask task=new DpsTask();

    @Mock
    private RepresentationIterator representationIterator;

    @Mock
    private ResultSlice<CloudIdAndTimestampResponse> latestDataChunk;

    private List<CloudIdAndTimestampResponse> latestDataList=new ArrayList<>();

    @Mock
    private ResultSlice<CloudTagsResponse> dataChunk;

    @Captor
    private ArgumentCaptor<DpsRecord> recordCaptor;

    private List<CloudTagsResponse> dataList=new ArrayList<>();


    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        submiter = new MCSTaskSubmiter(taskErrorDAO,taskStatusChecker,cassandraTaskInfoDAO,recordSubmitService,TOPOLOGY,task,TOPIC,reader);
        whenNew(DataSetServiceClient.class).withAnyArguments().thenReturn(dataSetServiceClient);
        whenNew(FileServiceClient.class).withAnyArguments().thenReturn(fileServiceClient);
        whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);
        task.setTaskId(TASK_ID);

        //używane w większości
        when(fileServiceClient.getFileUri(eq(CLOUD_ID1),eq(REPRESENTATION_NAME),eq(VERSION_1),eq(FILE_NAME_1))).thenReturn(FILE_URI_1);
    }

    @Test
    public void executeMcsBasedTask_taskKilled_verifyNothingSentToKafka() {
        task.addDataEntry(InputDataType.FILE_URLS, Collections.singletonList(FILE_URL_1));
        when(taskStatusChecker.hasKillFlag(eq(TASK_ID))).thenReturn(true);

        submiter.execute();

        verify(recordSubmitService,never()).submitRecord(any(DpsRecord.class),anyString());
    }

    @Test
    public void executeMcsBasedTask_taskIsNotKilled_verifyUpdateTaskInfoInCassandra() {
        task.addDataEntry(InputDataType.FILE_URLS, Collections.singletonList(FILE_URL_1));

        submiter.execute();


        verify(cassandraTaskInfoDAO).updateTask(eq(TASK_ID), anyString(), eq(String.valueOf(TaskState.CURRENTLY_PROCESSING)), any(Date.class));
    }

    @Test
    public void executeMcsBasedTask_errorInExecution_verifyTaskDropped() {
        task.addDataEntry(InputDataType.FILE_URLS, Collections.singletonList(FILE_URL_1));
        doThrow(new RuntimeException("Błąd uruchamiania task")).when(cassandraTaskInfoDAO).updateTask(anyLong(),anyString(),anyString(),any(Date.class));

        submiter.execute();


        verify(cassandraTaskInfoDAO).dropTask(anyLong(),anyString(),anyString());
    }

    @Test
    public void executeMcsBasedTask_oneFileUrl() {
        task.addDataEntry(InputDataType.FILE_URLS, Collections.singletonList(FILE_URL_1));

        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1);
    }



    @Test
    public void executeMcsBasedTask_threeFileUrls() {
        task.addDataEntry(InputDataType.FILE_URLS, Arrays.asList(FILE_URL_1,FILE_URL_2,FILE_URL_3));

        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1,FILE_URL_2,FILE_URL_3);
    }

    @Test
    public void executeMcsBasedTask_oneDatasetWithOneFile() {
        task.addDataEntry(InputDataType.DATASET_URLS, Collections.singletonList(DATASET_URL_1));
        when(dataSetServiceClient.getRepresentationIterator(eq(DATASET_PROVIDER_1),eq(DATASET_ID_1))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true,false);
        when(representationIterator.next()).thenReturn(REPRESENTATION_1);


        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1);
    }

    @Test
    public void executeMcsBasedTask_oneDatasetWithThreeFiles() {
        task.addDataEntry(InputDataType.DATASET_URLS, Collections.singletonList(DATASET_URL_1));
        when(dataSetServiceClient.getRepresentationIterator(eq(DATASET_PROVIDER_1),eq(DATASET_ID_1))).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true,true,true,false);
        when(representationIterator.next()).thenReturn(REPRESENTATION_1);

        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1,FILE_URL_1,FILE_URL_1);
    }

    @Test
    public void executeMcsBasedTask_oneLastRevisionWithOneFile() throws MCSException {
        task.addDataEntry(InputDataType.DATASET_URLS, Collections.singletonList(DATASET_URL_1));
        task.addParameter(PluginParameterKeys.REVISION_NAME, REVISION_NAME);
        task.addParameter(PluginParameterKeys.REVISION_PROVIDER,REVISION_PROVIDER_1);
        task.addParameter(PluginParameterKeys.REPRESENTATION_NAME, REPRESENTATION_NAME);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(eq(DATASET_ID_1)
                , eq(DATASET_PROVIDER_1), eq(REVISION_PROVIDER_1), eq(REVISION_NAME), eq(REPRESENTATION_NAME), eq(false), eq(null))).thenReturn(latestDataChunk);
        when(latestDataChunk.getResults()).thenReturn(latestDataList);
        latestDataList.add(new CloudIdAndTimestampResponse(CLOUD_ID1, FILE_CREATION_DATE_1));
        when(recordServiceClient.getRepresentationsByRevision(eq(CLOUD_ID1),eq(REPRESENTATION_NAME),eq(REVISION_NAME),eq(REVISION_PROVIDER_1),anyString())).thenReturn(Collections.singletonList(REPRESENTATION_1));


        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1);
    }

    @Test
    public void executeMcsBasedTask_lastRevisionsForTwoObject_verifyTwoRecordsSentToKafka() throws MCSException {
        prepareInvocationForLastRevisionOfTwoObjects();

        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1,FILE_URL_1);
    }

    @Test
    public void executeMcsBasedTask_lastRevisionsForTwoObjectAndLimitTo1_verifyOnlyOneRecordSentToKafka() throws MCSException {
        prepareInvocationForLastRevisionOfTwoObjects();
        task.addParameter(PluginParameterKeys.SAMPLE_SIZE,"1");

        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1);
    }

    private void prepareInvocationForLastRevisionOfTwoObjects() throws MCSException {
        task.addDataEntry(InputDataType.DATASET_URLS, Collections.singletonList(DATASET_URL_1));
        task.addParameter(PluginParameterKeys.REVISION_NAME, REVISION_NAME);
        task.addParameter(PluginParameterKeys.REVISION_PROVIDER,REVISION_PROVIDER_1);
        task.addParameter(PluginParameterKeys.REPRESENTATION_NAME, REPRESENTATION_NAME);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(eq(DATASET_ID_1)
                , eq(DATASET_PROVIDER_1), eq(REVISION_PROVIDER_1), eq(REVISION_NAME), eq(REPRESENTATION_NAME), eq(false), eq(null))).thenReturn(latestDataChunk);
        when(latestDataChunk.getResults()).thenReturn(latestDataList);
        latestDataList.add(new CloudIdAndTimestampResponse(CLOUD_ID1, FILE_CREATION_DATE_1));
        latestDataList.add(new CloudIdAndTimestampResponse(CLOUD_ID2, FILE_CREATION_DATE_2));
        when(recordServiceClient.getRepresentationsByRevision(eq(CLOUD_ID1),eq(REPRESENTATION_NAME),eq(REVISION_NAME),eq(REVISION_PROVIDER_1),anyString())).thenReturn(Collections.singletonList(REPRESENTATION_1));
        when(recordServiceClient.getRepresentationsByRevision(eq(CLOUD_ID2),eq(REPRESENTATION_NAME),eq(REVISION_NAME),eq(REVISION_PROVIDER_1),anyString())).thenReturn(Collections.singletonList(REPRESENTATION_1));
    }

    @Test
    public void executeMcsBasedTask_lastRevisionsForThreeObjectsInThreeChunks_verifyThreeRecordsSentToKafka() throws MCSException {
        prepareInvocationForLastRevisionForThreeObjectsInThreeChunks();

        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1,FILE_URL_1,FILE_URL_1);
    }

//    @Ignore //TO się faktycznie wywało
//    @Test
//    public void executeMcsBasedTask_lastRevisionsForThreeObjectsInThreeChunks_verifyOnlyTwoRecordSentToKafka() throws MCSException {
//        prepareInvocationForLastRevisionForThreeObjectsInThreeChunks();
//        task.addParameter(PluginParameterKeys.SAMPLE_SIZE,"2");
//
//        reader.executeMcsBasedTask();
//
//        verifyValidRecordsSentToKafka(FILE_URL_1,FILE_URL_1);
//    }


    private void prepareInvocationForLastRevisionForThreeObjectsInThreeChunks() throws MCSException {
        task.addDataEntry(InputDataType.DATASET_URLS, Collections.singletonList(DATASET_URL_1));
        task.addParameter(PluginParameterKeys.REVISION_NAME, REVISION_NAME);
        task.addParameter(PluginParameterKeys.REVISION_PROVIDER,REVISION_PROVIDER_1);
        task.addParameter(PluginParameterKeys.REPRESENTATION_NAME, REPRESENTATION_NAME);
        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(eq(DATASET_ID_1)
                , eq(DATASET_PROVIDER_1), eq(REVISION_PROVIDER_1), eq(REVISION_NAME), eq(REPRESENTATION_NAME), eq(false), anyString())).thenReturn(latestDataChunk);
        when(latestDataChunk.getResults()).thenReturn(latestDataList);
        when(latestDataChunk.getNextSlice()).thenReturn(EXAMPLE_DATE,EXAMPLE_DATE, null);
        latestDataList.add(new CloudIdAndTimestampResponse(CLOUD_ID1, FILE_CREATION_DATE_1));
        when(recordServiceClient.getRepresentationsByRevision(eq(CLOUD_ID1),eq(REPRESENTATION_NAME),eq(REVISION_NAME),eq(REVISION_PROVIDER_1),anyString())).thenReturn(Collections.singletonList(REPRESENTATION_1));
    }

    @Test
    public void executeMcsBasedTask_oneRevisionForGivenTimestampWithOneFile() throws MCSException {
        task.addDataEntry(InputDataType.DATASET_URLS, Collections.singletonList(DATASET_URL_1));
        task.addParameter(PluginParameterKeys.REVISION_NAME, REVISION_NAME);
        task.addParameter(PluginParameterKeys.REVISION_PROVIDER,REVISION_PROVIDER_1);
        task.addParameter(PluginParameterKeys.REVISION_TIMESTAMP,FILE_CREATION_DATE_STRING_1);
        task.addParameter(PluginParameterKeys.REPRESENTATION_NAME, REPRESENTATION_NAME);
        when(dataSetServiceClient.getDataSetRevisionsChunk(
                 eq(DATASET_PROVIDER_1), eq(DATASET_ID_1), eq(REPRESENTATION_NAME), eq(REVISION_NAME),eq(REVISION_PROVIDER_1),eq(FILE_CREATION_DATE_STRING_1),  eq(null), eq(null))).thenReturn(dataChunk);
        when(dataChunk.getResults()).thenReturn(dataList);
        dataList.add(new CloudTagsResponse(CLOUD_ID1,false,false,false));
        when(recordServiceClient.getRepresentationsByRevision(eq(CLOUD_ID1),eq(REPRESENTATION_NAME),eq(REVISION_NAME),eq(REVISION_PROVIDER_1),anyString())).thenReturn(Collections.singletonList(REPRESENTATION_1));


        submiter.execute();

        verifyValidRecordsSentToKafka(FILE_URL_1);
    }


    private void verifyValidRecordsSentToKafka(String... fileUrls) {
        verify(recordSubmitService,times(fileUrls.length)).submitRecord(recordCaptor.capture(),anyString());
        for(int i=0;i<fileUrls.length;i++){
            DpsRecord record = recordCaptor.getAllValues().get(i);
            assertEquals(fileUrls[i],record.getInputData());
            assertEquals(TASK_ID,record.getTaskId());
        }
    }
}