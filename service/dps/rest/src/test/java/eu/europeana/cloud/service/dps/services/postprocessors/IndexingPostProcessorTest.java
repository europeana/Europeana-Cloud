package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsBatchCleaner;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.stream.Stream;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.INCREMENTAL_INDEXING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IndexingPostProcessorTest {

  public static final String RECORD_ID_1 = "recordId1";
  private static final String RECORD_ID_2 = "recordId2";

  public static final String METIS_DATASET_ID = "metisDS_id";
  @Mock
  private HarvestedRecordsDAO harvestedRecordsDAO;

  @Mock
  private TaskStatusUpdater taskStatusUpdater;

  @Mock
  private TaskStatusChecker taskStatusChecker;

  @Mock
  private DatasetCleaner datasetCleaner;

  @InjectMocks
  private IndexingPostProcessor service;

  private final TaskInfo taskInfo = new TaskInfo();

  @Test
  void shouldCleanDateAndMd5ForPreviewAndForOneRecord() {
    try (
            MockedConstruction<DatasetCleaner> ignored =
                    Mockito.mockConstruction(DatasetCleaner.class, (datasetCleaner, context) -> {
                      when(datasetCleaner.getRecordsCount()).thenReturn(1);
                      when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
                    });

            MockedConstruction<HarvestedRecordsBatchCleaner> batchCleanerMock =
                    Mockito.mockConstruction(HarvestedRecordsBatchCleaner.class, (harvestedRecordsBatchCleaner, context) -> {
                      when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(1);
                    })
    ) {


      // when
      service.execute(taskInfo, prepareTaskForPreviewEnv());
      HarvestedRecordsBatchCleaner harvestedRecordsBatchCleaner = batchCleanerMock.constructed().get(0);

      // then
      verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
      verify(harvestedRecordsBatchCleaner).close();
      verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(1));
      verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(1));
      verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
    }
    //given
  }

  @Test
  void shouldCleanDateAndMd5ForPreviewAndForMultipleRecords() {
    try (
            MockedConstruction<DatasetCleaner> ignored =
                    Mockito.mockConstruction(DatasetCleaner.class, (datasetCleaner, context) -> {
                      when(datasetCleaner.getRecordsCount()).thenReturn(2);
                      when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
                    });

            MockedConstruction<HarvestedRecordsBatchCleaner> batchCleanerMock =
                    Mockito.mockConstruction(HarvestedRecordsBatchCleaner.class, (batchCleaner, context) -> {
                      when(batchCleaner.getCleanedCount()).thenReturn(2);
                    })
    ) {

      //when
      service.execute(taskInfo, prepareTaskForPreviewEnv());
      HarvestedRecordsBatchCleaner harvestedRecordsBatchCleaner = batchCleanerMock.constructed().get(0);

      //then
      verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
      verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_2);
      verify(harvestedRecordsBatchCleaner).close();
      verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(2));
      verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(2));
      verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
    }
  }

  @Test
  void shouldCleanDateAndMd5ForPublishAndForOneRecord() {
    try (
            MockedConstruction<DatasetCleaner> ignored =
                    Mockito.mockConstruction(DatasetCleaner.class, (datasetCleaner, context) -> {
                      when(datasetCleaner.getRecordsCount()).thenReturn(1);
                      when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
                    });

            MockedConstruction<HarvestedRecordsBatchCleaner> batchCleanerMock =
                    Mockito.mockConstruction(HarvestedRecordsBatchCleaner.class, (batchCleaner, context) -> {
                      when(batchCleaner.getCleanedCount()).thenReturn(1);
                    })
    ) {

      //when
      service.execute(taskInfo, prepareTaskForPublishEnv());
      HarvestedRecordsBatchCleaner harvestedRecordsBatchCleaner = batchCleanerMock.constructed().get(0);

      //then
      verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
      verify(harvestedRecordsBatchCleaner).close();
      verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(1));
      verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(1));
      verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
    }
  }

  @Test
  void shouldCleanDateAndMd5ForPublishAndMultipleRecords() {
    try (
            MockedConstruction<DatasetCleaner> ignored =
                    Mockito.mockConstruction(DatasetCleaner.class, (datasetCleaner, context) -> {
                      when(datasetCleaner.getRecordsCount()).thenReturn(2);
                      when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
                    });

            MockedConstruction<HarvestedRecordsBatchCleaner> batchCleanerMock =
                    Mockito.mockConstruction(HarvestedRecordsBatchCleaner.class, (batchCleaner, context) -> {
                      when(batchCleaner.getCleanedCount()).thenReturn(2);
                    })
    ) {

      //when
      service.execute(taskInfo, prepareTaskForPublishEnv());
      HarvestedRecordsBatchCleaner harvestedRecordsBatchCleaner = batchCleanerMock.constructed().get(0);

      //then
      verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
      verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_2);
      verify(harvestedRecordsBatchCleaner).close();
      verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(2));
      verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(2));
      verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
    }
  }

  @Test
  void shouldThrowPostProcessingExceptionInCaseCountsFail() throws IndexingException {
    when(datasetCleaner.getRecordsCount()).thenThrow(new IndexerRelatedIndexingException("Could not get record count"));
    DpsTask task = prepareTaskForNotUnknownEnv();
    assertThrows(PostProcessingException.class, () -> service.execute(taskInfo, task));
  }

  @Test
  void shouldThrowPostProcessingExceptionInCaseRecordIdsFail() throws IndexingException {
    when(datasetCleaner.getRecordsCount()).thenReturn(1);
    when(datasetCleaner.getRecordIds()).thenThrow(new IndexerRelatedIndexingException("Could not get record count"));
    DpsTask task = prepareTaskForNotUnknownEnv();
    assertThrows(PostProcessingException.class, () -> service.execute(taskInfo, task));
  }

  @Test
  void shouldThrowPostProcessingExceptionInCaseOfNotRecognizedEnvironment() {
    //given
    when(harvestedRecordsDAO.findDatasetRecords(METIS_DATASET_ID)).thenReturn(Collections.emptyIterator());
    //when
    DpsTask task = prepareTaskForNotUnknownEnv();
    assertThrows(PostProcessingException.class, () -> service.execute(taskInfo, task));
  }

  @Test
  void shouldNotStartPostprocessingForDroppedTask() {
    //given
    DpsTask task = prepareTaskForPreviewEnv();
    when(taskStatusChecker.hasDroppedStatus(anyLong())).thenReturn(true);
    doThrow(new TaskDroppedException(task)).when(taskStatusChecker).checkNotDropped(task);

    //when
    assertThrows(TaskDroppedException.class, () -> service.execute(taskInfo, task));

    //then
    verifyNoInteractions(taskStatusUpdater);
    verifyNoInteractions(harvestedRecordsDAO);
  }

  @Test
  void shouldParseIsoRecordDateProperly() {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
    dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");

    try (
            MockedConstruction<DatasetCleaner> ignored =
                    Mockito.mockConstruction(DatasetCleaner.class, (datasetCleaner, context) -> {
                      when(datasetCleaner.getRecordsCount()).thenReturn(1);
                      assertEquals(Date.from(Instant.parse("2020-06-14T16:46:00.000Z")), ((DataSetCleanerParameters) context.arguments().get(1)).getCleaningDate());
                    });
    ) {
      service.execute(taskInfo, dpsTask);
    }
  }

  @Test
  void shouldParseIsoRecordDateWithoutMillisecondsProperly() {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
    dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00Z");
    try (
            MockedConstruction<DatasetCleaner> ignored =
                    Mockito.mockConstruction(DatasetCleaner.class, (datasetCleaner, context) -> {
                      when(datasetCleaner.getRecordsCount()).thenReturn(1);
                      assertEquals(Date.from(Instant.parse("2020-06-14T16:46:00.000Z")), ((DataSetCleanerParameters) context.arguments().get(1)).getCleaningDate());
                    });
    ) {
      service.execute(taskInfo, dpsTask);
    }

  }

  @Test
  void shouldNeedsPostProcessingReturnTrueForNoIncrementalParamSet() {
    DpsTask task = new DpsTask();

    boolean result = service.needsPostProcessing(task);

    assertTrue(result);
  }

  @Test
  void shouldNeedsPostProcessingReturnTrueForFullIndexing() {
    DpsTask task = new DpsTask();
    task.addParameter(INCREMENTAL_INDEXING, "false");

    boolean result = service.needsPostProcessing(task);

    assertTrue(result);
  }

  @Test
  void shouldNeedsPostProcessingReturnFalseForIncrementalIndexing() {
    DpsTask task = new DpsTask();
    task.addParameter(INCREMENTAL_INDEXING, "true");

    boolean result = service.needsPostProcessing(task);

    assertFalse(result);
  }

  private DpsTask prepareTaskForPreviewEnv() {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
    dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");
    return dpsTask;
  }

  private DpsTask prepareTaskForPublishEnv() {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
    dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");
    return dpsTask;
  }

  private DpsTask prepareTaskForNotUnknownEnv() {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "Unknown_env");
    dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2021-06-14T16:47:00.000Z");
    return dpsTask;
  }


}