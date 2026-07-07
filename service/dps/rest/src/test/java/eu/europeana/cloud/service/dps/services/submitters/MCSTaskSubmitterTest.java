package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.net.URI;
import java.time.Instant;
import java.util.*;

import static eu.europeana.cloud.service.dps.Constants.DPS_REPRESENTATION_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MCSTaskSubmitterTest {

  public static final String REPRESENTATION_URI_STRING_1 = "http://localhost:8080/mcs/records/Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA/representations/mcsReaderRepresentation/versions/ec3c18b0-7354-11ea-b16e-04922659f621";
  public static final URI REPRESENTATION_ALL_VERSION_URI_1 = URI.create(
          "http://localhost:8080/mcs/records/Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA/representations/mcsReaderRepresentation/versions");
  private static final long TASK_ID = 10L;
  private static final String DATASET_PROVIDER_1 = "providerId1";
  private static final String DATASET_ID_1 = "datasetId1";
  private static final String TOPIC = "topic1";
  private static final String TOPOLOGY = "validation_topology";
  private static final String SCHEMA_NAME = "schema1";
  //File 1
  private static final String FILE_URL_1 = "http://localhost:8080/mcs/records/Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA/representations/mcsReaderRepresentation/versions/ec3c18b0-7354-11ea-b16e-04922659f621/files/9da076ce-f382-4bbf-8b6a-c80d943aa46a";
  private static final URI FILE_URI_1 = URI.create(FILE_URL_1);
  private static final String FILE_NAME_1 = "9da076ce-f382-4bbf-8b6a-c80d943aa46a";
  private static final String FILE_CREATION_DATE_STRING_1 = "2020-03-31T15:38:38.873Z";
  public static final Date FILE_CREATION_DATE_1 = Date.from(Instant.parse(FILE_CREATION_DATE_STRING_1));
  private static final String CLOUD_ID1 = "Z5T3UYERNLKRLLII5EW42NNCCPPTVQV2MKNDF4VL7UBKBVI2JHRA";
  private static final File FILE_1 = new File(FILE_NAME_1, "text/plain", "81d15fa0095586d8e40c7698604753aa",
      FILE_CREATION_DATE_STRING_1, 3403L, FILE_URI_1);
  private static final String VERSION_1 = "ec3c18b0-7354-11ea-b16e-04922659f621";
  private static final URI REPRESENTATON_URI_1 = URI.create(REPRESENTATION_URI_STRING_1);
  private static final Representation REPRESENTATION_1 = new Representation(
      CLOUD_ID1,
      DPS_REPRESENTATION_NAME,
      VERSION_1,
      REPRESENTATION_ALL_VERSION_URI_1,
      REPRESENTATON_URI_1,
      DATASET_PROVIDER_1,
      Collections.singletonList(FILE_1),
      false,
      FILE_CREATION_DATE_1,
      DATASET_ID_1, false);

  private static final String FILE_URL_2 = "http://localhost:8080/mcs/records/YI3S73BZBO2ZPINWZ62RBLAJSATKUG3O2YF4UWYC23BM6CDVBTMA/representations/mcsReaderRepresentation/versions/ec64af50-7354-11ea-b16e-04922659f621/files/0b936b8f-1e43-47ca-986b-7d2cce366c33";

  private static final String FILE_URL_3 = "http://localhost:8080/mcs/records/YGF5ZH7GCHRSMJPVQKXOYULUCVJATJ3FOZE2KWV7MXYNZEITSJ5Q/representations/mcsReaderRepresentation/versions/ebe93dc0-7354-11ea-b16e-04922659f621/files/8a9db572-5217-486f-9a96-6dd3c4f149dd";

  @Mock
  private TaskStatusChecker taskStatusChecker;

  @Mock
  private TaskStatusUpdater taskStatusUpdater;

  @Mock
  private RecordExecutionSubmitService recordKafkaSubmitService;

  @Mock
  private ProcessedRecordsDAO processedRecordsDAO;

  private MCSTaskSubmitter submitter;

  private final DpsTask task = new DpsTask();

  @Mock
  private RepresentationIterator representationIterator;

  @Captor
  private ArgumentCaptor<DpsRecord> recordCaptor;

  private SubmitTaskParameters submitParameters;

  @BeforeEach
  void setup() {
    RecordSubmitService recordSubmitService = new RecordSubmitService(processedRecordsDAO, recordKafkaSubmitService);
    submitter = new MCSTaskSubmitter(taskStatusChecker, taskStatusUpdater, recordSubmitService, null, null, null);
    task.setTaskId(TASK_ID);
    task.addParameter(PluginParameterKeys.SCHEMA_NAME, SCHEMA_NAME);
    submitParameters = SubmitTaskParameters.builder()
            .task(task)
            .taskInfo(TaskInfo.builder()
                    .topologyName(TOPOLOGY)
                    .build())
            .topicName(TOPIC).build();
  }

  @FunctionalInterface
  interface ThrowingRunnable {
    void run() throws Exception;
  }

  private void withClientMocks(ThrowingRunnable testCode) throws Exception {
    withClientMocks(null, testCode);
  }

  private void withClientMocks(
                               ThrowingConsumer<RecordServiceClient> recordClientSetup, ThrowingRunnable testCode) throws Exception {
    try (
            MockedConstruction<DataSetServiceClient> ignored1 = Mockito.mockConstruction(DataSetServiceClient.class,
                    (mock, context) -> {
                      when(mock.getRepresentationIterator(
                          DATASET_PROVIDER_1, DATASET_ID_1))
                          .thenReturn(representationIterator);
                    });
            MockedConstruction<FileServiceClient> ignored2 = Mockito.mockConstruction(FileServiceClient.class,
                    (mock, context) -> {
                      when(mock.getFileUri(CLOUD_ID1, DPS_REPRESENTATION_NAME, VERSION_1, FILE_NAME_1))
                              .thenReturn(FILE_URI_1);
                    });
            MockedConstruction<RecordServiceClient> ignored3 = Mockito.mockConstruction(RecordServiceClient.class,
                    (mock, context) -> {
                      if (recordClientSetup != null) recordClientSetup.accept(mock);
                    })
    ) {
      testCode.run();
    }
  }

  @Test
  void executeMcsBasedTask_taskKilled_verifyNothingSentToKafka() throws Exception {
    withClientMocks(() -> {
      task.setInput(prepateBatchInput());
      doThrow(new TaskDroppedException(task)).when(taskStatusChecker).checkNotDropped(any());

      submitter.execute(submitParameters);

      verify(recordKafkaSubmitService, never()).submitRecord(any(DpsRecord.class), anyString());
    });
  }

  @Test
  void executeMcsBasedTask_taskIsNotKilled_verifyUpdateTaskInfoInCassandra() throws Exception {
    mockRepresentationIterator(List.of(createRepresentation(FILE_URL_1)));
    withClientMocks(() -> {
      task.setInput(prepateBatchInput());

      submitter.execute(submitParameters);

      verify(taskStatusUpdater).updateStatusAndExpected(
              eq(TASK_ID),
              eq(EngineTaskState.QUEUED),
              argThat(counters ->
                      counters.getExpectedRecords() == 1
                              && counters.getExpectedDepublishRecords() == 0
              )
      );
    });
  }

  @Test
  void executeMcsBasedTask_errorInExecution_verifyTaskDropped() throws Exception {
    withClientMocks(() -> {
      task.setInput(prepateBatchInput());
      doThrow(new RuntimeException("Error in task execution")).when(recordKafkaSubmitService)
              .submitRecord(any(DpsRecord.class), anyString());

      submitter.execute(submitParameters);

      verify(taskStatusUpdater).setTaskDropped(anyLong(), anyString());
    });
  }

  @Test
  void executeMcsBasedTask_oneFileUrl() throws Exception {
    mockRepresentationIterator(List.of(createRepresentation(FILE_URL_1)));
    withClientMocks(() -> {
      task.setInput(prepateBatchInput());

      submitter.execute(submitParameters);

      verifyValidTaskSent(FILE_URL_1);
    });
  }

  @Test
  void executeMcsBasedTask_threeFileUrls() throws Exception {
    mockRepresentationIterator(List.of(createRepresentation(FILE_URL_1),
        createRepresentation(FILE_URL_2),createRepresentation(FILE_URL_3)));
    withClientMocks(() -> {
      task.setInput(prepateBatchInput());

      submitter.execute(submitParameters);

      verifyValidTaskSent(FILE_URL_1, FILE_URL_2, FILE_URL_3);
    });
  }

  @Test
  void executeMcsBasedTask_3000FileUrls() throws Exception {
    List<String> fileUrls = new ArrayList<>();
    List<Representation> representations=new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      fileUrls.add(FILE_URL_1);
      representations.add(createRepresentation(FILE_URL_1));
    }
    mockRepresentationIterator(representations);
    withClientMocks(() -> {
      task.setInput(prepateBatchInput());

      submitter.execute(submitParameters);

      verifyValidTaskSent(fileUrls.toArray(new String[0]));
    });
  }

  @Test
  void executeMcsBasedTask_oneDatasetWithOneFile() throws Exception {
    withClientMocks(
            null,

            () -> {
              task.setInput(prepateBatchInput());

              when(representationIterator.hasNext()).thenReturn(true, false);
              when(representationIterator.next()).thenReturn(REPRESENTATION_1);

              submitter.execute(submitParameters);

              verifyValidTaskSent(FILE_URL_1);
            }
    );
  }


  @Test
  void executeMcsBasedTask_oneDatasetWithThreeFiles() throws Exception {
    withClientMocks(null,
            () -> {
              task.setInput(prepateBatchInput());
              when(representationIterator.hasNext()).thenReturn(true, true, true, false);
              when(representationIterator.next()).thenReturn(REPRESENTATION_1);

              submitter.execute(submitParameters);

              verifyValidTaskSent(FILE_URL_1, FILE_URL_1, FILE_URL_1);
            }
    );

  }

  private static BatchInfo prepateBatchInput() {
    return BatchInfo.builder()
                    .providerId(DATASET_PROVIDER_1)
                    .batchId(DATASET_ID_1)
                    .build();
  }

  private void verifyValidTaskSent(String... fileUrls) {
    verifyValidRecordsSentToKafka(fileUrls);
    verifyValidStateAndExpectedSizeSavedInCassandra(fileUrls);
  }

  private void verifyValidRecordsSentToKafka(String[] fileUrls) {
    verify(recordKafkaSubmitService, times(fileUrls.length)).submitRecord(recordCaptor.capture(), anyString());
    for (int i = 0; i < fileUrls.length; i++) {
      DpsRecord record = recordCaptor.getAllValues().get(i);
      assertEquals(fileUrls[i], record.getRecordId());
      assertEquals(TASK_ID, record.getTaskId());
      assertEquals(SCHEMA_NAME, record.getMetadataPrefix());
    }
  }

  private void verifyValidStateAndExpectedSizeSavedInCassandra(String[] fileUrls) {
    verify(taskStatusUpdater).updateStatusAndExpected(
            eq(TASK_ID),
            eq(EngineTaskState.QUEUED),
            argThat(counters ->
                    counters.getExpectedRecords() == fileUrls.length
                            && counters.getExpectedDepublishRecords() == 0
            )
    );
  }

  private void verifyValidStateAndExpectedSizeSavedInCassandraForDeletedFiles(String[] fileUrls) {
    verify(taskStatusUpdater).updateStatusAndExpected(
            eq(TASK_ID),
            eq(EngineTaskState.QUEUED),
            argThat(counters ->
                    counters.getExpectedRecords() == 0
                            && counters.getExpectedDepublishRecords() == fileUrls.length
            )
    );
  }

  private void mockRepresentationIterator(List<Representation> representations) {
    Iterator<Representation> innerIterator = representations.iterator();
    when(representationIterator.hasNext()).thenAnswer(invocation->innerIterator.hasNext());
    when(representationIterator.next()).thenAnswer(invocation->innerIterator.next());
  }

  private static Representation createRepresentation(String fileUrl) {
    Representation representation=new Representation();
    File file=new File();
    file.setContentUri(URI.create(fileUrl));
    representation.setFiles(List.of(file));
    return representation;
  }
}