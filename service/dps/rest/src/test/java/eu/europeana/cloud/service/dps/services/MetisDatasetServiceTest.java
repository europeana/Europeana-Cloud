package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.MetisDataset;
import eu.europeana.cloud.service.dps.metis.indexing.MetisDataSetParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetStatsRetriever;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingEnvironment;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class MetisDatasetServiceTest {

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForDefaultPreviewDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("1")
                                .targetIndexingDatabase(TargetIndexingDatabase.PREVIEW)
                                .targetIndexingEnvironment(TargetIndexingEnvironment.DEFAULT)
                                .build())));

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("1")
                        .build(),
                TargetIndexingDatabase.PREVIEW,
                TargetIndexingEnvironment.DEFAULT);
        Assert.assertEquals(10, metisDataset.getSize());
    }

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForAltPreviewDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("2")
                                .targetIndexingDatabase(TargetIndexingDatabase.PREVIEW)
                                .targetIndexingEnvironment(TargetIndexingEnvironment.ALTERNATIVE)
                                .build())));

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("2")
                        .build(),
                TargetIndexingDatabase.PREVIEW,
                TargetIndexingEnvironment.ALTERNATIVE);
        Assert.assertEquals(10, metisDataset.getSize());
    }

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForDefaultPublishDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("3")
                                .targetIndexingDatabase(TargetIndexingDatabase.PUBLISH)
                                .targetIndexingEnvironment(TargetIndexingEnvironment.DEFAULT)
                                .build())));

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("3")
                        .build(),
                TargetIndexingDatabase.PUBLISH,
                TargetIndexingEnvironment.DEFAULT);
        Assert.assertEquals(10, metisDataset.getSize());
    }

    @Test
    public void shouldPrepareCorrectStatsForMetisDatasetForAltPublishDatabase() throws IndexingException {
        DatasetStatsRetriever datasetStatsRetriever = mock(DatasetStatsRetriever.class);
        doReturn(10L).when(datasetStatsRetriever).getTotalRecordsForDataset(
                argThat(samePropertyValuesAs(
                        MetisDataSetParameters.builder()
                                .dataSetId("4")
                                .targetIndexingDatabase(TargetIndexingDatabase.PUBLISH)
                                .targetIndexingEnvironment(TargetIndexingEnvironment.ALTERNATIVE)
                                .build())));

        MetisDatasetService metisDatasetService = new MetisDatasetService(datasetStatsRetriever);
        MetisDataset metisDataset = metisDatasetService.prepareStatsFor(
                MetisDataset.builder()
                        .id("4")
                        .build(),
                TargetIndexingDatabase.PUBLISH,
                TargetIndexingEnvironment.ALTERNATIVE);
        Assert.assertEquals(10, metisDataset.getSize());
    }
}