package eu.europeana.cloud.service.dps.services.task.postprocessors;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

@RunWith(MockitoJUnitRunner.class)
public class IndexingPostProcessorTest {

    @Mock
    private HarvestedRecordsDAO harvestedRecordsDAO;

    @Mock
    private TaskStatusUpdater taskStatusUpdater;

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

    private static final Date latestHarvestDateForRecord_3 = Date.from(LocalDateTime.of(2020, 6, 14, 16, 47).toInstant(ZoneOffset.UTC));
    private static final Date publishedHarvestDateForRecord_3 = Date.from(LocalDateTime.of(2020, 6, 14, 16, 47).toInstant(ZoneOffset.UTC));
    private static final Date previewHarvestDateForRecord_3 = Date.from(LocalDateTime.of(2020, 6, 14, 16, 47).toInstant(ZoneOffset.UTC));
    private static final UUID latestHarvestMd5ForRecord_3 = UUID.fromString("28dcf591-d007-11eb-92d1-fa163e64bb83");
    private static final UUID publishHarvestMd5ForRecord_3 = UUID.fromString("28dcf591-d007-11eb-92d1-000000000001");
    private static final UUID previewHarvestMd5ForRecord_3= UUID.fromString("28dcf591-d007-11eb-92d1-000000000002");


    @Test
    public void shouldCleanDateAndMd5ForPreviewAndForOneRecord() {
        //given
        List<HarvestedRecord> allHarvestedRecords = new ArrayList<>();
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build());
        when(harvestedRecordsDAO.findDatasetRecords("metisDS_id")).thenReturn(allHarvestedRecords.iterator());
        //when
        service.execute(prepareTaskForPreviewEnv());
        //then
        verify(harvestedRecordsDAO).findDatasetRecords(any());
        verify(harvestedRecordsDAO).insertHarvestedRecord(argThat(samePropertyValuesAs(
                HarvestedRecord.builder()
                        .metisDatasetId("metisDS_id")
                        .recordLocalId("recordId")
                        .latestHarvestDate(latestHarvestDateForRecord_1)
                        .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                        .publishedHarvestDate(publishedHarvestDateForRecord_1)
                        .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                        .previewHarvestDate(null)
                        .previewHarvestMd5(null)
                        .build())
        ));
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(),anyString());
    }

    @Test
    public void shouldCleanDateAndMd5ForPreviewAndForMultipleRecords() {
        //given
        List<HarvestedRecord> allHarvestedRecords = new ArrayList<>();
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build());
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(publishedHarvestDateForRecord_2)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_2)
                .previewHarvestDate(previewHarvestDateForRecord_2)
                .previewHarvestMd5(previewHarvestMd5ForRecord_2)
                .build());
        when(harvestedRecordsDAO.findDatasetRecords("metisDS_id")).thenReturn(allHarvestedRecords.iterator());
        //when
        service.execute(prepareTaskForPreviewEnv());
        //then
        ArgumentCaptor<HarvestedRecord> argument = ArgumentCaptor.forClass(HarvestedRecord.class);
        verify(harvestedRecordsDAO,times(2)).insertHarvestedRecord(argument.capture());
        List<HarvestedRecord> values = argument.getAllValues();

        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(null)
                .previewHarvestMd5(null)
                .build()));

        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(publishedHarvestDateForRecord_2)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_2)
                .previewHarvestDate(null)
                .previewHarvestMd5(null)
                .build()));

        verify(harvestedRecordsDAO).findDatasetRecords(any());
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(),anyString());
    }

    @Test
    public void shouldCleanDateAndMd5ForPublishAndForOneRecord() {
        //given
        List<HarvestedRecord> allHarvestedRecords = new ArrayList<>();
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build());
        when(harvestedRecordsDAO.findDatasetRecords("metisDS_id")).thenReturn(allHarvestedRecords.iterator());
        //when
        service.execute(prepareTaskForPublishEnv());
        //then
        verify(harvestedRecordsDAO).findDatasetRecords(any());
        verify(harvestedRecordsDAO).insertHarvestedRecord(argThat(samePropertyValuesAs(
                HarvestedRecord.builder()
                        .metisDatasetId("metisDS_id")
                        .recordLocalId("recordId")
                        .latestHarvestDate(latestHarvestDateForRecord_1)
                        .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                        .publishedHarvestDate(null)
                        .publishedHarvestMd5(null)
                        .previewHarvestDate(previewHarvestDateForRecord_1)
                        .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                        .build())
        ));
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(),anyString());
    }

    @Test
    public void shouldCleanDateAndMd5ForPublishAndMultipleRecords() {
        //given
        List<HarvestedRecord> allHarvestedRecords = new ArrayList<>();
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(publishedHarvestDateForRecord_1)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_1)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build());
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(publishedHarvestDateForRecord_2)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_2)
                .previewHarvestDate(previewHarvestDateForRecord_2)
                .previewHarvestMd5(UUID.fromString("28dcf591-d007-11eb-92d1-000000000002"))
                .build());
        when(harvestedRecordsDAO.findDatasetRecords("metisDS_id")).thenReturn(allHarvestedRecords.iterator());
        //when
        service.execute(prepareTaskForPublishEnv());
        //then
        verify(harvestedRecordsDAO).findDatasetRecords(any());
        //
        ArgumentCaptor<HarvestedRecord> argument = ArgumentCaptor.forClass(HarvestedRecord.class);
        verify(harvestedRecordsDAO,times(2)).insertHarvestedRecord(argument.capture());
        List<HarvestedRecord> values = argument.getAllValues();

        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_1)
                .latestHarvestMd5(latestHarvestMd5ForRecord_1)
                .publishedHarvestDate(null)
                .publishedHarvestMd5(null)
                .previewHarvestDate(previewHarvestDateForRecord_1)
                .previewHarvestMd5(previewHarvestMd5ForRecord_1)
                .build()));
        Assert.assertTrue(values.contains(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_2)
                .latestHarvestMd5(latestHarvestMd5ForRecord_2)
                .publishedHarvestDate(null)
                .publishedHarvestMd5(null)
                .previewHarvestDate(previewHarvestDateForRecord_2)
                .previewHarvestMd5(previewHarvestMd5ForRecord_2)
                .build()));

        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(),anyString());
    }

    @Test
    public void shouldDropTheTaskInCaseOfNotRecognizedEnvironment() {
        //given
        List<HarvestedRecord> allHarvestedRecords = new ArrayList<>();
        when(harvestedRecordsDAO.findDatasetRecords("metisDS_id")).thenReturn(allHarvestedRecords.iterator());
        //when
        service.execute(prepareTaskForNotUnknownEnv());
        //then
        verify(taskStatusUpdater, never()).setTaskCompletelyProcessed(anyLong(), anyString());
        verify(harvestedRecordsDAO,never()).insertHarvestedRecord(any());
        verify(taskStatusUpdater).setTaskDropped(anyLong(), anyString());
    }

    @Test
    public void shouldDoNothingForRecordWithDateBeforeCutDateAndForPublishEnv() {
        //given
        List<HarvestedRecord> allHarvestedRecords = new ArrayList<>();
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_3)
                .latestHarvestMd5(latestHarvestMd5ForRecord_3)
                .publishedHarvestDate(publishedHarvestDateForRecord_3)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_3)
                .previewHarvestDate(previewHarvestDateForRecord_3)
                .previewHarvestMd5(previewHarvestMd5ForRecord_3)
                .build());
        when(harvestedRecordsDAO.findDatasetRecords("metisDS_id")).thenReturn(allHarvestedRecords.iterator());
        //when
        service.execute(prepareTaskForPublishEnv());
        //then
        verify(harvestedRecordsDAO).findDatasetRecords(any());
        //
        verify(harvestedRecordsDAO,never()).insertHarvestedRecord(any());
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(),anyString());
    }

    @Test
    public void shouldDoNothingForRecordWithDateBeforeCutDateAndForPreviewEnv() {
        //given
        List<HarvestedRecord> allHarvestedRecords = new ArrayList<>();
        allHarvestedRecords.add(HarvestedRecord.builder()
                .metisDatasetId("metisDS_id")
                .recordLocalId("recordId")
                .latestHarvestDate(latestHarvestDateForRecord_3)
                .latestHarvestMd5(latestHarvestMd5ForRecord_3)
                .publishedHarvestDate(publishedHarvestDateForRecord_3)
                .publishedHarvestMd5(publishHarvestMd5ForRecord_3)
                .previewHarvestDate(previewHarvestDateForRecord_3)
                .previewHarvestMd5(previewHarvestMd5ForRecord_3)
                .build());
        when(harvestedRecordsDAO.findDatasetRecords("metisDS_id")).thenReturn(allHarvestedRecords.iterator());
        //when
        service.execute(prepareTaskForPreviewEnv());
        //then
        verify(harvestedRecordsDAO).findDatasetRecords(any());
        //
        verify(harvestedRecordsDAO,never()).insertHarvestedRecord(any());
        verify(taskStatusUpdater).setTaskCompletelyProcessed(anyLong(),anyString());
    }

    private DpsTask prepareTaskForPreviewEnv(){
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS_id");
        dpsTask.addParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "false");
        dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");
        return dpsTask;
    }

    private DpsTask prepareTaskForPublishEnv(){
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS_id");
        dpsTask.addParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "false");
        dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
        dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2020-06-14T16:46:00.000Z");

//        private static final Date latestHarvestDateForRecord_3 = Date.from(LocalDateTime.of(2020, 6, 14, 16, 47).toInstant(ZoneOffset.UTC));
        return dpsTask;
    }

    private DpsTask prepareTaskForNotUnknownEnv(){
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.METIS_DATASET_ID, "metisDS_id");
        dpsTask.addParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "false");
        dpsTask.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "Unknown_env");
        dpsTask.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "2021-06-14T16:47:00.000Z");
        return dpsTask;
    }


}