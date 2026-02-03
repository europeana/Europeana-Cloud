package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.MetisDataset;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetStatsRetriever;
import eu.europeana.cloud.service.dps.metis.indexing.MetisDataSetParameters;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

class MetisDatasetServiceTest {

    @Test
    void shouldPrepareCorrectStatsForMetisDatasetForDefaultPreviewDatabase() throws IndexingException {
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
        assertEquals(10, metisDataset.getSize());
  }

    @Test
    void shouldPrepareCorrectStatsForMetisDatasetForAltPreviewDatabase() throws IndexingException {
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
        assertEquals(10, metisDataset.getSize());
  }

    @Test
    void shouldPrepareCorrectStatsForMetisDatasetForDefaultPublishDatabase() throws IndexingException {
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
        assertEquals(10, metisDataset.getSize());
  }

    @Test
    void shouldPrepareCorrectStatsForMetisDatasetForAltPublishDatabase() throws IndexingException {
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
        assertEquals(10, metisDataset.getSize());
  }

    @Test
    void shouldReturnEmptyListOfRecordsForEmptyDataset() {
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

        assertEquals(0, harvestedRecords.size());
  }

    @Test
    void shouldReturnEmptyListOfRecordsForNonEmptyDatasetWithNoPublishedRecords() {
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

        assertEquals(0, harvestedRecords.size());
  }

    @Test
    void shouldReturnListOrRecordsForNonEmptyDatasetWithSomePublishedRecords() {
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

        assertEquals(1, harvestedRecords.size());
        assertEquals("1", harvestedRecords.get(0).getMetisDatasetId());
        assertEquals("1", harvestedRecords.get(0).getRecordLocalId());
        assertEquals(publishedDate, harvestedRecords.get(0).getPublishedHarvestDate());
    }

    @Test
    void shouldReturnListOfRecordsForNonEmptyDatasetWithAllPublishedRecords() {
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

        assertEquals(2, harvestedRecords.size());
        assertEquals("1", harvestedRecords.get(0).getMetisDatasetId());
        assertEquals("1", harvestedRecords.get(0).getRecordLocalId());
        assertEquals(publishedDateForRecord1, harvestedRecords.get(0).getPublishedHarvestDate());

        assertEquals("1", harvestedRecords.get(1).getMetisDatasetId());
        assertEquals("2", harvestedRecords.get(1).getRecordLocalId());
        assertEquals(publishedDateForRecord2, harvestedRecords.get(1).getPublishedHarvestDate());
    }

    @Test
    void shouldReturnEmptyListOrRecordsForNonExistingDataset() {
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

        assertEquals(0, harvestedRecords.size());
  }
}