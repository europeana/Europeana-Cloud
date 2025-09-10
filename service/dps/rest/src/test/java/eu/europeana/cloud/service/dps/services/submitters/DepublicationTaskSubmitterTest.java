package eu.europeana.cloud.service.dps.services.submitters;

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER;
import static org.hamcrest.beans.SamePropertyValuesAs.samePropertyValuesAs;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.DepublicationFilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DepublicationTaskSubmitterTest {

  private static final String TOPIC_NAME = "mock_topic";
  private static final long TASK_ID = 50;
  private static final String DATASET_ID = "10";
  private static final String RECORD_ID_1 = "/10/recordId1";
  private static final String RECORD_ID_2 = "/10/recordId2";
  private static final DpsRecord RECORD_1 = DpsRecord.builder().taskId(TASK_ID).recordId(RECORD_ID_1).build();
  private static final DpsRecord RECORD_2 = DpsRecord.builder().taskId(TASK_ID).recordId(RECORD_ID_2).build();

  @Mock
  private FilesCounterFactory filesCounterFactory;
  @Mock
  private TaskStatusUpdater taskStatusUpdater;
  @Mock
  private IndexWrapper indexWrapper;
  @Mock
  private Indexer<FullBeanImpl> indexer;
  @Mock
  private KafkaTopicSelector kafkaTopicSelector;
  @Mock
  private RecordSubmitService recordSubmitService;
  @Mock
  @SuppressWarnings("unused") //It is needed for @InjectMocks
  private TaskStatusChecker taskStatusChecker;

  @InjectMocks
  private DepublicationTaskSubmitter submitter;

  @Captor
  private ArgumentCaptor<SubmitTaskParameters> captore;

  private DpsTask dpsTask;
  private SubmitTaskParameters parameters;

  @Before
  public void setUp() {
    when(indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH)).thenReturn(indexer);

    DepublicationFilesCounter filesCounter = new DepublicationFilesCounter(indexWrapper);
    when(filesCounterFactory.createFilesCounter(any(), any())).thenReturn(filesCounter);

    when(kafkaTopicSelector.findPreferredTopicNameFor(any())).thenReturn(TOPIC_NAME);

    TaskInfo taskInfo = new TaskInfo();
    taskInfo.setExpectedRecordsNumber(UNKNOWN_EXPECTED_RECORDS_NUMBER);

    dpsTask = new DpsTask("taskName5");
    dpsTask.setTaskId(TASK_ID);

    parameters = SubmitTaskParameters.builder().taskInfo(taskInfo)
                                     .task(dpsTask)
                                     .build();
  }

  @Test
  public void shouldProperlySentTaskForSelectedRecords() throws TaskSubmissionException {
    dpsTask.getParameters().put(PluginParameterKeys.METIS_DATASET_ID, DATASET_ID);
    dpsTask.getParameters().put(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, RECORD_ID_1 + "," + RECORD_ID_2);
    when(recordSubmitService.submitRecord(any(), any())).thenReturn(true);

    submitter.submitTask(parameters);

    verify(taskStatusUpdater).updateSubmitParameters(captore.capture());
    SubmitTaskParameters updatedParameters = captore.getValue();
    assertEquals(TOPIC_NAME, updatedParameters.getTopicName());
    assertEquals(2, updatedParameters.getTaskInfo().getExpectedRecordsNumber());
    verify(recordSubmitService).submitRecord(argThat(samePropertyValuesAs(RECORD_1)), eq(parameters));
    verify(recordSubmitService).submitRecord(argThat(samePropertyValuesAs(RECORD_2)), eq(parameters));
    verify(taskStatusUpdater).updateState(TASK_ID, TaskState.QUEUED);
  }

  @Test
  public void shouldProperlySentTaskForDataset() throws TaskSubmissionException, IndexingException {
    when(indexer.countRecords(DATASET_ID)).thenReturn(2L);
    when(indexer.getRecordIds(eq(DATASET_ID), any(), any())).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
    dpsTask.getParameters().put(PluginParameterKeys.METIS_DATASET_ID, DATASET_ID);
    when(recordSubmitService.submitRecord(any(), any())).thenReturn(true);

    submitter.submitTask(parameters);

    verify(taskStatusUpdater).updateSubmitParameters(captore.capture());
    SubmitTaskParameters updatedParameters = captore.getValue();
    assertEquals(TOPIC_NAME, updatedParameters.getTopicName());
    assertEquals(2, updatedParameters.getTaskInfo().getExpectedRecordsNumber());
    verify(recordSubmitService).submitRecord(argThat(samePropertyValuesAs(RECORD_1)), eq(parameters));
    verify(recordSubmitService).submitRecord(argThat(samePropertyValuesAs(RECORD_2)), eq(parameters));
    verify(taskStatusUpdater).updateState(TASK_ID, TaskState.QUEUED);
  }

  @Test
  public void shouldDropTaskWhenDatasetIsEmpty() throws TaskSubmissionException, IndexingException {
    when(indexer.countRecords(DATASET_ID)).thenReturn(0L);
    dpsTask.getParameters().put(PluginParameterKeys.METIS_DATASET_ID, DATASET_ID);

    submitter.submitTask(parameters);

    verify(taskStatusUpdater).setTaskDropped(eq(TASK_ID), anyString());
    verify(recordSubmitService, never()).submitRecord(any(), any());
  }

  @Test
  public void shouldNotSentRecordsWhenTaskIsCanceled() {
    dpsTask.getParameters().put(PluginParameterKeys.METIS_DATASET_ID, DATASET_ID);
    dpsTask.getParameters().put(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, RECORD_ID_1 + "," + RECORD_ID_2);
    when(taskStatusChecker.hasDroppedStatus(TASK_ID)).thenReturn(true);

    assertThrows(TaskDroppedException.class, () -> submitter.submitTask(parameters));

    verify(recordSubmitService, never()).submitRecord(any(), any());
  }

}