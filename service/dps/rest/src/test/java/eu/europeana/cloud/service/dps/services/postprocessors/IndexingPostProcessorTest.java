package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

@RunWith(PowerMockRunner.class)
@PrepareForTest(IndexingPostProcessor.class)
public class IndexingPostProcessorTest {

    public static final String RECORD_ID_1 = "recordId1";
    private static final String RECORD_ID_2 = "recordId2";

    public static final String METIS_DATASET_ID = "metisDS_id";
    @Mock
    private HarvestedRecordsDAO harvestedRecordsDAO;

    @Mock
    private TaskStatusUpdater taskStatusUpdater;

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
        when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, RECORD_ID_1)).thenReturn(Optional.of(oneRecord));
        when(datasetCleaner.getRecordsCount()).thenReturn(1);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
        //when
        service.execute(taskInfo, prepareTaskForPreviewEnv());
        //then
        verify(harvestedRecordsDAO).findRecord(any(), any());
        verify(harvestedRecordsDAO).insertHarvestedRecord(argThat(samePropertyValuesAs(
                HarvestedRecord.builder()
                        .metisDatasetId(METIS_DATASET_ID)
                        .recordLocalId(RECORD_ID_1)
                        .latestHarvestDate(latestHarvestDateForRecord_1)
                        .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                        .publishedHarvestDate(publishedHarvestDateForRecord_1)
                        .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                        .previewHarvestDate(null)
                        .previewHarvestMd5(null)
                        .build())
        ));
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
        when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, RECORD_ID_1)).thenReturn(Optional.of(record1));
        when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, RECORD_ID_2)).thenReturn(Optional.of(record2));
        when(datasetCleaner.getRecordsCount()).thenReturn(2);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
        //when
        service.execute(taskInfo,  prepareTaskForPreviewEnv());
        //then
        ArgumentCaptor<HarvestedRecord> argument = ArgumentCaptor.forClass(HarvestedRecord.class);
        verify(harvestedRecordsDAO, times(2)).insertHarvestedRecord(argument.capture());
        List<HarvestedRecord> values = argument.getAllValues();

        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(null)
                .previewHarvestMd5(null)
                .build()));

        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(publishedHarvestDateForRecord_2)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_2)
                .previewHarvestDate(null)
                .previewHarvestMd5(null)
                .build()));

        verify(harvestedRecordsDAO, times(2)).findRecord(any(), any());
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
        when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, RECORD_ID_1)).thenReturn(Optional.of(oneRecord));
        when(datasetCleaner.getRecordsCount()).thenReturn(1);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1));
        //when
        service.execute(taskInfo,  prepareTaskForPublishEnv());
        //then
        verify(harvestedRecordsDAO).findRecord(any(), any());
        verify(harvestedRecordsDAO).insertHarvestedRecord(argThat(samePropertyValuesAs(
                HarvestedRecord.builder()
                        .metisDatasetId(METIS_DATASET_ID)
                        .recordLocalId(RECORD_ID_1)
                        .latestHarvestDate(latestHarvestDateForRecord_1)
                        .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                        .publishedHarvestDate(null)
                        .publishedHarvestMd5(null)
                        .previewHarvestDate(previewHarvestDateForRecord_1)
                        .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                        .build())
        ));
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
        when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, RECORD_ID_1)).thenReturn(Optional.of(record1));
        when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, RECORD_ID_2)).thenReturn(Optional.of(record2));
        when(datasetCleaner.getRecordsCount()).thenReturn(2);
        when(datasetCleaner.getRecordIds()).thenReturn(Stream.of(RECORD_ID_1, RECORD_ID_2));
        //when
        service.execute(taskInfo, prepareTaskForPublishEnv());
        //then
        ArgumentCaptor<HarvestedRecord> argument = ArgumentCaptor.forClass(HarvestedRecord.class);
        verify(harvestedRecordsDAO, times(2)).insertHarvestedRecord(argument.capture());
        List<HarvestedRecord> values = argument.getAllValues();

        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(null)
                .publishedHarvestMd5(null)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build()));

        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId(METIS_DATASET_ID)
                .recordLocalId(RECORD_ID_1)
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(null)
                .publishedHarvestMd5(null)
                .previewHarvestDate(previewHarvestDateForRecord_2)
                .previewHarvestMd5(previewHarvestMd5ForRecord_2)
                .build()));

        verify(harvestedRecordsDAO, times(2)).findRecord(any(), any());
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

    private DpsTask prepareTaskForPreviewEnv() {
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
        dpsTask.addParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "false");
        dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");
        return dpsTask;
    }

    private DpsTask prepareTaskForPublishEnv() {
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
        dpsTask.addParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "false");
        dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
        dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");

//        private static final Date latestHarvestDateForRecord_3 = Date.from(LocalDateTime.of(2020, 6, 14, 16, 47).toInstant(ZoneOffset.UTC));
        return dpsTask;
    }

    private DpsTask prepareTaskForNotUnknownEnv() {
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
        dpsTask.addParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "false");
        dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "Unknown_env");
        dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2021-06-14T16:47:00.000Z");
        return dpsTask;
    }


}