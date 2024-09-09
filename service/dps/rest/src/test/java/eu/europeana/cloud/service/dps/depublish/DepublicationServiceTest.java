package eu.europeana.cloud.service.dps.depublish;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DepublicationService.class, DatasetDepublisher.class, TestContext.class,})
public class DepublicationServiceTest {

  private static final long TASK_ID = 1000L;
  private static final int EXPECTED_SET_SIZE = 100;
  private static final String DATASET_METIS_ID = "metisSetId";
  public static final long WAITING_FOR_COMPLETE_TIME = 7000L;
  public static final String RECORD1 = "/1/item1";
  public static final String RECORD2 = "/1/item2";
  private static final Date HARVEST_DATE = new Date();
  private static final UUID RECORD1_MD5 = UUID.fromString("0acbca5d-f561-31a8-ab40-27521caadbc4");

  private SubmitTaskParameters parameters;

  @Autowired
  private DepublicationService service;

  @Autowired
  private TaskStatusUpdater updater;

  @Autowired
  private TaskStatusChecker taskStatusChecker;

  @Autowired
  private RecordStatusUpdater recordStatusUpdater;

  @Autowired
  private HarvestedRecordsDAO harvestedRecordsDAO;

  @Autowired
  private IndexWrapper indexWrapper;

  private Indexer indexer;
  @Before
  public void setup() throws IndexingException {
    service.setProgressPollingPeriod(100);
    indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH);
    Mockito.reset(updater, taskStatusChecker, indexer, recordStatusUpdater);
    MockitoAnnotations.initMocks(this);
    DpsTask task = new DpsTask();
    task.setTaskId(TASK_ID);
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, DATASET_METIS_ID);
    task.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, RECORD1 + "," + RECORD2);
    parameters = SubmitTaskParameters.builder()
                                     .taskInfo(TaskInfo.builder()
                                                       .expectedRecordsNumber(EXPECTED_SET_SIZE)
                                                       .build())
                                     .task(task).build();
    when(indexer.countRecords(anyString())).thenReturn((long) EXPECTED_SET_SIZE, 0L);
    when(indexer.removeAll(anyString(), nullable(Date.class))).thenReturn(EXPECTED_SET_SIZE);
    when(indexer.remove(anyString())).thenReturn(true);
    when(indexer.indexTombstone(anyString())).thenReturn(true);
  }

  @Test
  public void shouldInvokeTaskRemoveOnIndexer() throws IndexingException {
    service.depublishDataset(parameters);

    verify(indexer).removeAll(eq(DATASET_METIS_ID), isNull());
    assertTaskSucceed();
  }

  @Test
  public void shouldCleanPublishedHarvestDateAndMd5WhileDepublicateWholeDataset() {
    reset(harvestedRecordsDAO);
    when(harvestedRecordsDAO.findDatasetRecords(DATASET_METIS_ID))
        .thenReturn(Collections.singleton(
                                   HarvestedRecord.builder().metisDatasetId(DATASET_METIS_ID).recordLocalId(RECORD1)
                                                  .latestHarvestDate(HARVEST_DATE).latestHarvestMd5(RECORD1_MD5)
                                                  .previewHarvestDate(HARVEST_DATE).previewHarvestMd5(RECORD1_MD5)
                                                  .publishedHarvestDate(HARVEST_DATE).publishedHarvestMd5(RECORD1_MD5).build())
                               .iterator());

    service.depublishDataset(parameters);

    verify(harvestedRecordsDAO).insertHarvestedRecord(
        HarvestedRecord.builder().metisDatasetId(DATASET_METIS_ID).recordLocalId(RECORD1)
                       .latestHarvestDate(HARVEST_DATE).latestHarvestMd5(RECORD1_MD5)
                       .previewHarvestDate(HARVEST_DATE).previewHarvestMd5(RECORD1_MD5).build());
    assertTaskSucceed();
  }

  @Test
  public void shouldSaveValidSetSizeInResults() {
    service.depublishDataset(parameters);

    verify(updater).setUpdateProcessedFiles(TASK_ID, EXPECTED_SET_SIZE, 0, 0, 0, 0);
  }

  @Test
  public void shouldWaitForAllRowsRemoved() throws IndexingException {
    AtomicBoolean allRowsRemoved = new AtomicBoolean(false);
    StopWatch watch = StopWatch.createStarted();

    when(indexer.countRecords(anyString())).then(r -> {
      allRowsRemoved.set(watch.getTime() > WAITING_FOR_COMPLETE_TIME);
      if (allRowsRemoved.get()) {
        return 0L;
      } else {
        return (long) EXPECTED_SET_SIZE;
      }
    });

    service.depublishDataset(parameters);

    assertTaskSucceed();
    assertTrue(allRowsRemoved.get());
  }

  @Test
  public void shouldNotInvokeTaskRemoveIfTaskWereKilledBefore() throws IndexingException {
    when(taskStatusChecker.hasDroppedStatus(anyLong())).thenReturn(true);

    service.depublishDataset(parameters);

    verify(indexer, never()).removeAll(eq(DATASET_METIS_ID), isNull());
    verify(updater, never()).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
  }

  @Test
  public void shouldTaskFailWhenRemoveMethodThrowsException() throws IndexingException {
    when(indexer.removeAll(anyString(), nullable(Date.class))).thenThrow(
        new IndexerRelatedIndexingException("Indexer exception!"));

    service.depublishDataset(parameters);

    assertTaskFailed();
  }

  @Test
  public void shouldTaskFailWhenRemovedRowCountNotMatchExpected() throws IndexingException {
    when(indexer.removeAll(anyString(), nullable(Date.class))).thenReturn(EXPECTED_SET_SIZE + 2);

    service.depublishDataset(parameters);

    assertTaskFailed();
  }

  //////////////////////////////record depublish///////////////////////////////////////

  @Test
  public void shouldRemoveRecordBeInvokedForEveryRecord() throws IndexingException {
    service.depublishIndividualRecords(parameters);

    verify(indexer).remove(RECORD1);
    verify(indexer).remove(RECORD2);
    assertTaskSucceed();
  }

  @Test
  public void shouldCleanPublishedHarvestDateAndMd5WhileDepublicateChosenRecords() {
    //given
    HarvestedRecord theRecord = HarvestedRecord.builder().metisDatasetId(DATASET_METIS_ID).recordLocalId(RECORD1)
                                               .latestHarvestDate(HARVEST_DATE).latestHarvestMd5(RECORD1_MD5)
                                               .previewHarvestDate(HARVEST_DATE).previewHarvestMd5(RECORD1_MD5)
                                               .publishedHarvestDate(HARVEST_DATE).publishedHarvestMd5(RECORD1_MD5).build();

    when(harvestedRecordsDAO.findDatasetRecords(anyString())).thenReturn(Collections.singleton(theRecord).iterator());

    //when
    service.depublishDataset(parameters);

    //then
    verify(harvestedRecordsDAO).insertHarvestedRecord(
        HarvestedRecord.builder().metisDatasetId(DATASET_METIS_ID).recordLocalId(RECORD1)
                       .latestHarvestDate(HARVEST_DATE).latestHarvestMd5(RECORD1_MD5)
                       .previewHarvestDate(HARVEST_DATE).previewHarvestMd5(RECORD1_MD5).build());
    assertTaskSucceed();
  }

  @Test
  public void shouldValidRecordCountBeSavedInResult() {
    service.depublishIndividualRecords(parameters);

    verify(updater).setUpdateProcessedFiles(TASK_ID, 2, 0, 0, 0, 0);
    verify(recordStatusUpdater).addSuccessfullyProcessedRecord(1, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD1);
    verify(recordStatusUpdater).addSuccessfullyProcessedRecord(2, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD2);
    assertTaskSucceed();
  }

  @Test
  public void shouldHaltTheDepublicationProcessInCaseOfTombstoneCreationIssues() throws IndexingException {
    when(indexer.indexTombstone(anyString())).thenReturn(false);
    service.depublishIndividualRecords(parameters);

    verify(updater).setUpdateProcessedFiles(TASK_ID, 1, 0, 0, 1, 0);
    verify(recordStatusUpdater).addWronglyProcessedRecord(eq(1), eq(TASK_ID), eq(TopologiesNames.DEPUBLICATION_TOPOLOGY),
        any(), any(), any());
    verify(recordStatusUpdater).addWronglyProcessedRecord(eq(1), eq(TASK_ID), eq(TopologiesNames.DEPUBLICATION_TOPOLOGY),
        any(), any(), any());
    assertTaskSucceed();
    when(indexer.indexTombstone(anyString())).thenReturn(true);
  }



  @Test
  public void shouldRecordBeFailedInResultsWhenExceptionIsThrownWhileRemoving() throws IndexingException {
    when(indexer.remove(RECORD1)).thenThrow(RecordRelatedIndexingException.class);
    when(indexer.remove(RECORD2)).thenReturn(true);

    service.depublishIndividualRecords(parameters);

    verify(updater).setUpdateProcessedFiles(TASK_ID, 2, 0, 0, 1, 0);
    verify(recordStatusUpdater).addWronglyProcessedRecord(eq(1), eq(TASK_ID), eq(TopologiesNames.DEPUBLICATION_TOPOLOGY),
        eq(RECORD1), any(), any());
    verify(recordStatusUpdater).addSuccessfullyProcessedRecord(2, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD2);
    assertTaskSucceed();
  }

  @Test
  public void shouldRecordBeFailedInResultsWhenRemoveRecordReturnsFalse() throws IndexingException {
    when(indexer.remove(RECORD1)).thenReturn(false);
    when(indexer.remove(RECORD2)).thenReturn(true);

    service.depublishIndividualRecords(parameters);

    verify(updater).setUpdateProcessedFiles(TASK_ID, 2, 0, 0, 1, 0);
    verify(recordStatusUpdater).addWronglyProcessedRecord(eq(1), eq(TASK_ID), eq(TopologiesNames.DEPUBLICATION_TOPOLOGY),
        eq(RECORD1), any(), any());
    verify(recordStatusUpdater).addSuccessfullyProcessedRecord(2, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD2);
    assertTaskSucceed();
  }

  @Test
  public void shouldInterruptPerformingWhileTaskIsKilled() throws IndexingException {
    AtomicBoolean taskKilled = new AtomicBoolean(false);
    when(indexer.remove(RECORD1)).thenAnswer(invocation -> {
      taskKilled.set(true);
      return true;
    });
    when(indexer.remove(RECORD2)).thenReturn(true);
    when(taskStatusChecker.hasDroppedStatus(anyLong())).thenAnswer(invocation -> taskKilled.get());

    service.depublishIndividualRecords(parameters);

    verify(updater, never()).setUpdateProcessedFiles(TASK_ID, 2, 0, 0, 0, 0);
    verify(recordStatusUpdater, never()).addSuccessfullyProcessedRecord(anyInt(), anyLong(), any(), eq(RECORD2));
  }

  private void assertTaskSucceed() {
    verify(updater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
  }

  private void assertTaskFailed() {
    verify(updater, never()).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    verify(updater).setTaskDropped(eq(TASK_ID), anyString());
  }

}