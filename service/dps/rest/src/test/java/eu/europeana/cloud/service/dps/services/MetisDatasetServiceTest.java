package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.MetisDataset;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetStatsRetriever;
import eu.europeana.cloud.service.dps.metis.indexing.MetisDataSetParameters;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class MetisDatasetServiceTest {

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForDefaultPreviewDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("1")
                                .targetIndexingDatabase(TargetIndexingDatabase.PREVIEW)
                                .build())));

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("1")
                        .build(),
                TargetIndexingDatabase.PREVIEW);
        Assert.assertEquals(10, metisDataset.getSize());
    }

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForAltPreviewDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("2")
                                .targetIndexingDatabase(TargetIndexingDatabase.PREVIEW)
                                .build())));

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("2")
                        .build(),
                TargetIndexingDatabase.PREVIEW);
        Assert.assertEquals(10, metisDataset.getSize());
    }

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForDefaultPublishDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("3")
                                .targetIndexingDatabase(TargetIndexingDatabase.PUBLISH)
                                .build())));

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("3")
                        .build(),
                TargetIndexingDatabase.PUBLISH);
        Assert.assertEquals(10, metisDataset.getSize());
    }

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForAltPublishDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("4")
                                .targetIndexingDatabase(TargetIndexingDatabase.PUBLISH)
                                .build()
                        )
                )
        );

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("4")
                        .build(),
                TargetIndexingDatabase.PUBLISH);
        Assert.assertEquals(10, metisDataset.getSize());
    }

    @Test
    public void shouldReturnEmptyListOfRecordsForEmptyDataset() {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);
        doReturn(Optional.empty()).when(harvestedRecordsDAO).findRecord(anyString(), anyString());

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        List<HarvestedRecord> harvestedRecords = metisDatasetService.findPublishedRecordsInSet(
                MetisDataset
                        .builder()
                        .id("1")
                        .build(),
                List.of("1", "2"));

        Assert.assertEquals(0, harvestedRecords.size());
    }

    @Test
    public void shouldReturnEmptyListOfRecordsForNonEmptyDatasetWithNoPublishedRecords() {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);

        doReturn(Optional.of(HarvestedRecord
                .builder()
                .metisDatasetId("1")
                .build())).when(harvestedRecordsDAO).findRecord("1", "1");

        doReturn(Optional.of(HarvestedRecord
                .builder()
                .metisDatasetId("1")
                .build())).when(harvestedRecordsDAO).findRecord("1", "2");

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        List<HarvestedRecord> harvestedRecords = metisDatasetService.findPublishedRecordsInSet(
                MetisDataset
                        .builder()
                        .id("1")
                        .build(),
                List.of("1", "2"));

        Assert.assertEquals(0, harvestedRecords.size());
    }

    @Test
    public void shouldReturnListOrRecordsForNonEmptyDatasetWithSomePublishedRecords() {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);

        Date publishedDate = new Date();
        doReturn(Optional.of(HarvestedRecord
                .builder()
                .metisDatasetId("1")
                .recordLocalId("1")
                .publishedHarvestDate(publishedDate)
                .build())).when(harvestedRecordsDAO).findRecord("1", "1");

        doReturn(Optional.of(HarvestedRecord
                .builder()
                .metisDatasetId("1")
                .build())).when(harvestedRecordsDAO).findRecord("1", "2");

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        List<HarvestedRecord> harvestedRecords = metisDatasetService.findPublishedRecordsInSet(
                MetisDataset
                        .builder()
                        .id("1")
                        .build(),
                List.of("1", "2"));

        Assert.assertEquals(1, harvestedRecords.size());
        Assert.assertEquals("1", harvestedRecords.get(0).getMetisDatasetId());
        Assert.assertEquals("1", harvestedRecords.get(0).getRecordLocalId());
        Assert.assertEquals(publishedDate, harvestedRecords.get(0).getPublishedHarvestDate());
    }

    @Test
    public void shouldReturnListOfRecordsForNonEmptyDatasetWithAllPublishedRecords() {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);

        Date publishedDateForRecord1 = new Date();
        Date publishedDateForRecord2 = new Date();
        doReturn(Optional.of(HarvestedRecord
                .builder()
                .metisDatasetId("1")
                .recordLocalId("1")
                .publishedHarvestDate(publishedDateForRecord1)
                .build())).when(harvestedRecordsDAO).findRecord("1", "1");

        doReturn(Optional.of(HarvestedRecord
                .builder()
                .metisDatasetId("1")
                .recordLocalId("2")
                .publishedHarvestDate(publishedDateForRecord2)
                .build())).when(harvestedRecordsDAO).findRecord("1", "2");

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        List<HarvestedRecord> harvestedRecords = metisDatasetService.findPublishedRecordsInSet(
                MetisDataset
                        .builder()
                        .id("1")
                        .build(),
                List.of("1", "2"));

        Assert.assertEquals(2, harvestedRecords.size());
        Assert.assertEquals("1", harvestedRecords.get(0).getMetisDatasetId());
        Assert.assertEquals("1", harvestedRecords.get(0).getRecordLocalId());
        Assert.assertEquals(publishedDateForRecord1, harvestedRecords.get(0).getPublishedHarvestDate());

        Assert.assertEquals("1", harvestedRecords.get(1).getMetisDatasetId());
        Assert.assertEquals("2", harvestedRecords.get(1).getRecordLocalId());
        Assert.assertEquals(publishedDateForRecord2, harvestedRecords.get(1).getPublishedHarvestDate());
    }

    @Test
    public void shouldReturnEmptyListOrRecordsForNonExistingDataset() {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        HarvestedRecordsDAO harvestedRecordsDAO = mock(HarvestedRecordsDAO.class);

        doReturn(Optional.empty()).when(harvestedRecordsDAO).findRecord(anyString(), anyString());

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever, harvestedRecordsDAO);
        List<HarvestedRecord> harvestedRecords = metisDatasetService.findPublishedRecordsInSet(
                MetisDataset
                        .builder()
                        .id("1")
                        .build(),
                List.of("1", "2", "3", "4"));

        Assert.assertEquals(0, harvestedRecords.size());
    }
}