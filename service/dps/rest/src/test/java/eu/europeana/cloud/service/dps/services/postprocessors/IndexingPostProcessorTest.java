package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsBatchCleaner;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;

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

    private static final Date latestHarvestDateForRecord_1 = Date.from(LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC));
    private static final Date publishedHarvestDateForRecord_1 = Date.from(LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC));
    private static final Date previewHarvestDateForRecord_1 = Date.from(LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC));
    private static final UUID latestHarvestMd5ForRecord_1 = UUID.fromString("28dcf591-d007-11eb-92d1-fa163e64bb83");
    private static final UUID publishHarvestMd5ForRecord_1 = UUID.fromString("28dcf591-d007-11eb-92d1-000000000001");
    private static final UUID previewHarvestMd5ForRecord_1 = UUID.fromString("28dcf591-d007-11eb-92d1-000000000002");

    private static final Date latestHarvestDateForRecord_2 = Date.from(LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC));
    private static final Date publishedHarvestDateForRecord_2 = Date.from(LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC));
    private static final Date previewHarvestDateForRecord_2 = Date.from(LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC));
    private static final UUID latestHarvestMd5ForRecord_2 = UUID.fromString("28dcf591-d007-11eb-92d1-fa163e64bb83");
    private static final UUID publishHarvestMd5ForRecord_2 = UUID.fromString("28dcf591-d007-11eb-92d1-000000000001");
    private static final UUID previewHarvestMd5ForRecord_2 = UUID.fromString("28dcf591-d007-11eb-92d1-000000000002");
    private final TaskInfo taskInfo=new TaskInfo();

    @Before
    public void setup() throws Exception {
        PowerMockito.whenNew(DatasetCleaner.class).withAnyArguments().thenReturn(datasetCleaner);
        PowerMockito.whenNew(HarvestedRecordsBatchCleaner.class).withAnyArguments().thenReturn(harvestedRecordsBatchCleaner);
    }

    @Test
    public void shouldCleanDateAndMd5ForPreviewAndForOneRecord() throws SetupRelatedIndexingException, IndexerRelatedIndexingException {
        //given
        HarvestedRecord oneRecord = HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build();
        when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(1);
        when(datasetCleaner.getRecordsCount()).thenReturn(1);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
        //when
        service.execute(taskInfo, prepareTaskForPreviewEnv());
        //then
        verify(harvestedRecordsBatchCleaner).cleanRecord(RECORD_ID_1);
        verify(harvestedRecordsBatchCleaner).close();
        verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(1));
        verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(1));
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
    }

    @Test
    public void shouldCleanDateAndMd5ForPreviewAndForMultipleRecords() throws SetupRelatedIndexingException, IndexerRelatedIndexingException {
        //given
        HarvestedRecord record1 = HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build();
        HarvestedRecord record2 = HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_2)
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(publishedHarvestDateForRecord_2)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_2)
                .previewHarvestDate(previewHarvestDateForRecord_2)
                .previewHarvestMd5(previewHarvestMd5ForRecord_2)
                .build();
        when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(2);
        when(datasetCleaner.getRecordsCount()).thenReturn(2);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
        //when
        service.execute(taskInfo,  prepareTaskForPreviewEnv());
        //then
        verify(harvestedRecordsBatchCleaner).cleanRecord(RECORD_ID_1);
        verify(harvestedRecordsBatchCleaner).cleanRecord(RECORD_ID_2);
        verify(harvestedRecordsBatchCleaner).close();
        verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(2));
        verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(2));
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
    }

    @Test
    public void shouldCleanDateAndMd5ForPublishAndForOneRecord() throws SetupRelatedIndexingException, IndexerRelatedIndexingException {
        //given
        HarvestedRecord oneRecord = HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build();
        when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(1);
        when(datasetCleaner.getRecordsCount()).thenReturn(1);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
        //when
        service.execute(taskInfo,  prepareTaskForPublishEnv());
        //then
        verify(harvestedRecordsBatchCleaner).cleanRecord(RECORD_ID_1);
        verify(harvestedRecordsBatchCleaner).close();
        verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(anyLong(), eq(1));
        verify(taskStatusUpdater).updatePostProcessedRecordsCount(anyLong(), eq(1));
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(), anyString());
    }

    @Test
    public void shouldCleanDateAndMd5ForPublishAndMultipleRecords() throws SetupRelatedIndexingException, IndexerRelatedIndexingException {
        //given
        HarvestedRecord record1 = HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build();
        HarvestedRecord record2 = HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_2)
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(publishedHarvestDateForRecord_2)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_2)
                .previewHarvestDate(previewHarvestDateForRecord_2)
                .previewHarvestMd5(previewHarvestMd5ForRecord_2)
                .build();
        when(harvestedRecordsBatchCleaner.getCleanedCount()).thenReturn(2);
        when(datasetCleaner.getRecordsCount()).thenReturn(2);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
        //when
        service.execute(taskInfo, prepareTaskForPublishEnv());
        //then
        verify(harvestedRecordsBatchCleaner).cleanRecord(RECORD_ID_1);
        verify(harvestedRecordsBatchCleaner).cleanRecord(RECORD_ID_2);
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
        when(taskStatusChecker.hasDroppedStatus(anyLong())).thenReturn(true);
        //when
        service.execute(taskInfo, prepareTaskForPreviewEnv());
        //then
        verifyNoInteractions(taskStatusUpdater);
        verifyNoInteractions(harvestedRecordsDAO);
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