package eu.europeana.cloud.service.dps.services.postprocessors;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.INCREMENTAL_INDEXING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
import eu.europeana.indexing.exception.IndexingException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(IndexingPostProcessor.class)
@PowerMockIgnore({"org.apache.logging.log4j.*", "com.sun.org.apache.xerces.*", "eu.europeana.cloud.test.CassandraTestInstance"})
public class IndexingPostProcessorTest {

  public static final String RECORD_ID_1 = "recordId1";
  private static final String RECORD_ID_2 = "recordId2";

  public static final String METIS_DATASET_ID = "metisDS_id";
  @Mock
  private HarvestedRecordsDAO harvestedRecordsDAO;

  @Mock
  private HarvestedRecordsBatchCleaner harvestedRecordsBatchCleaner;

  @Mock
  private TaskStatusUpdater taskStatusUpdater;

  @Mock
  private TaskStatusChecker taskStatusChecker;

  @Mock
  private DatasetCleaner datasetCleaner;

  @InjectMocks
  private IndexingPostProcessor service;

  private final TaskInfo taskInfo = new TaskInfo();

  @Captor
  private ArgumentCaptor<DataSetCleanerParameters> captor;

  @Before
  public void setup() throws Exception {
    PowerMockito.whenNew(DatasetCleaner.class).withAnyArguments().thenReturn(datasetCleaner);
    PowerMockito.whenNew(HarvestedRecordsBatchCleaner.class).withAnyArguments().thenReturn(harvestedRecordsBatchCleaner);
  }

  @Test
  public void shouldCleanDateAndMd5ForPreviewAndForOneRecord() throws IndexingException {
    //given
    when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(1);
    when(datasetCleaner.getRecordsCount()).thenReturn(1);
    when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
    //when
    service.execute(taskInfo, prepareTaskForPreviewEnv());
    //then
    verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
    verify(harvestedRecordsBatchCleaner).close();
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(1));
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(1));
    verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
  }

  @Test
  public void shouldCleanDateAndMd5ForPreviewAndForMultipleRecords() throws IndexingException {
    //given
    when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(2);
    when(datasetCleaner.getRecordsCount()).thenReturn(2);
    when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
    //when
    service.execute(taskInfo, prepareTaskForPreviewEnv());
    //then
    verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
    verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_2);
    verify(harvestedRecordsBatchCleaner).close();
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(2));
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(2));
    verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
  }

  @Test
  public void shouldCleanDateAndMd5ForPublishAndForOneRecord() throws IndexingException {
    //given
    when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(1);
    when(datasetCleaner.getRecordsCount()).thenReturn(1);
    when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
    //when
    service.execute(taskInfo, prepareTaskForPublishEnv());
    //then
    verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
    verify(harvestedRecordsBatchCleaner).close();
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(1));
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(1));
    verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
  }

  @Test
  public void shouldCleanDateAndMd5ForPublishAndMultipleRecords() throws IndexingException {
    //given
    when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(2);
    when(datasetCleaner.getRecordsCount()).thenReturn(2);
    when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
    //when
    service.execute(taskInfo, prepareTaskForPublishEnv());
    //then
    verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_1);
    verify(harvestedRecordsBatchCleaner).executeRecord(RECORD_ID_2);
    verify(harvestedRecordsBatchCleaner).close();
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(2));
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(2));
    verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
  }

  @Test(expected = PostProcessingException.class)
  public void shouldThrowPostProcessingExceptionInCaseOfNotRecognizedEnvironment() {
    //given
    when(harvestedRecordsDAO.findDatasetRecords(METIS_DATASET_ID)).thenReturn(Collections.emptyIterator());
    //when
    service.execute(taskInfo, prepareTaskForNotUnknownEnv());
  }

  @Test
  public void shouldNotStartPostprocessingForDroppedTask() {
    //given
    DpsTask task = prepareTaskForPreviewEnv();
    when(taskStatusChecker.hasDroppedStatus(anyLong())).thenReturn(true);
    doThrow(new TaskDroppedException(task)).when(taskStatusChecker).checkNotDropped(task);

    //when
    assertThrows(TaskDroppedException.class,()-> service.execute(taskInfo, task));

    //then
    verifyNoInteractions(taskStatusUpdater);
    verifyNoInteractions(harvestedRecordsDAO);
  }

  @Test
  public void shouldParseIsoRecordDateProperly() throws Exception {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
    dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");

    service.execute(taskInfo, dpsTask);

    PowerMockito.verifyNew(DatasetCleaner.class).withArguments(any(), captor.capture());
    assertEquals(Date.from(Instant.parse("2020-06-14T16:46:00.000Z")), captor.getValue().getCleaningDate());
  }

  @Test
  public void shouldParseIsoRecordDateWithoutMillisecondsProperly() throws Exception {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
    dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00Z");

    service.execute(taskInfo, dpsTask);

    PowerMockito.verifyNew(DatasetCleaner.class).withArguments(any(), captor.capture());
    assertEquals(Date.from(Instant.parse("2020-06-14T16:46:00.000Z")), captor.getValue().getCleaningDate());
  }

  @Test
  public void shouldNeedsPostProcessingReturnTrueForNoIncrementalParamSet() {
    DpsTask task = new DpsTask();

    boolean result = service.needsPostProcessing(task);

    assertTrue(result);
  }

  @Test
  public void shouldNeedsPostProcessingReturnTrueForFullIndexing() {
    DpsTask task = new DpsTask();
    task.addParameter(INCREMENTAL_INDEXING, "false");

    boolean result = service.needsPostProcessing(task);

    assertTrue(result);
  }

  @Test
  public void shouldNeedsPostProcessingReturnFalseForIncrementalIndexing() {
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

    //        private static final Date latestHarvestDateForRecord_3 = Date.from(LocalDateTime.of(2020, 6, 14, 16, 47).toInstant(ZoneOffset.UTC));
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