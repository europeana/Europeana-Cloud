package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils;

import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordsDAO;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class HarvestedRecordCategorizationServiceTest {

    @Test
    public void shouldCategorizeRecordAsReadyForProcessingInCaseOfNoDefinitionInDB() {
        //given
        HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
        HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HarvestedRecordCategorizationService(harvestedRecordsDAO);

        Instant now = Instant.now();
        //when
        CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
                CategorizationParameters.builder()
                        .datasetId("exampleDatasetId")
                        .recordId("exampleRecordId")
                        .recordDateStamp(now)
                        .currentHarvestDate(now)
                        .build());
        //then
        verify(harvestedRecordsDAO, times(1)).findRecord(eq("exampleDatasetId"), eq("exampleRecordId"));
        verify(harvestedRecordsDAO, times(1)).insertHarvestedRecord(
                argThat(samePropertyValuesAs(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .latestHarvestDate(Date.from(now))
                                .build()
                )));
        assertTrue(categorizationResult.shouldBeProcessed());
    }

    @Test
    public void shouldCategorizeRecordAsReadyForProcessingInCaseOfExistingDefinitionInDBAndNewHarvestedRecord() {
        //given
        HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
        Instant dateOfHarvesting = Instant.now();
        when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
                Optional.of(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .publishedHarvestDate(Date.from(dateOfHarvesting.minus(1, ChronoUnit.DAYS)))
                                .build()
                ));
        HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HarvestedRecordCategorizationService(harvestedRecordsDAO);

        //when
        CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
                CategorizationParameters.builder()
                        .datasetId("exampleDatasetId")
                        .recordId("exampleRecordId")
                        .recordDateStamp(dateOfHarvesting)
                        .currentHarvestDate(dateOfHarvesting)
                        .build());
        //then
        verify(harvestedRecordsDAO, times(1)).findRecord(eq("exampleDatasetId"), eq("exampleRecordId"));
        verify(harvestedRecordsDAO, times(1)).insertHarvestedRecord(
                argThat(samePropertyValuesAs(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .latestHarvestDate(Date.from(dateOfHarvesting))
                                .publishedHarvestDate(Date.from(dateOfHarvesting.minus(1, ChronoUnit.DAYS)))
                                .build()
                )));
        assertTrue(categorizationResult.shouldBeProcessed());
    }

    @Test
    public void shouldCategorizeRecordAsReadyForProcessingUsingBuffer() {
        //given
        HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
        Instant dateOfHarvesting = Instant.now();
        when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
                Optional.of(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .publishedHarvestDate(Date.from(dateOfHarvesting))
                                .build()
                ));
        HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HarvestedRecordCategorizationService(harvestedRecordsDAO);

        //when
        CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
                CategorizationParameters.builder()
                        .datasetId("exampleDatasetId")
                        .recordId("exampleRecordId")
                        .recordDateStamp(dateOfHarvesting.minus(10, ChronoUnit.MINUTES))
                        .currentHarvestDate(dateOfHarvesting)
                        .build());
        //then
        verify(harvestedRecordsDAO, times(1)).findRecord(eq("exampleDatasetId"), eq("exampleRecordId"));
        verify(harvestedRecordsDAO, times(1)).insertHarvestedRecord(
                argThat(samePropertyValuesAs(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .latestHarvestDate(Date.from(dateOfHarvesting))
                                .publishedHarvestDate(Date.from(dateOfHarvesting))
                                .build()
                )));
        assertTrue(categorizationResult.shouldBeProcessed());
    }

    @Test
    public void shouldCategorizeRecordAsNotReadyForProcessingInCaseOfExistingDefinitionInDBAndOldRecord() {
        //given
        HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
        Date dateOfHarvesting = new Date();
        when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
                Optional.of(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .publishedHarvestDate(Date.from(dateOfHarvesting.toInstant().plus(3, ChronoUnit.DAYS)))
                                .build()
                ));
        HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HarvestedRecordCategorizationService(harvestedRecordsDAO);

        //when
        CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
                CategorizationParameters.builder()
                        .datasetId("exampleDatasetId")
                        .recordId("exampleRecordId")
                        .recordDateStamp(dateOfHarvesting.toInstant())
                        .currentHarvestDate(dateOfHarvesting.toInstant())
                        .build());
        //then
        verify(harvestedRecordsDAO, times(1)).findRecord(eq("exampleDatasetId"), eq("exampleRecordId"));
        verify(harvestedRecordsDAO, times(1)).insertHarvestedRecord(
                argThat(samePropertyValuesAs(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .latestHarvestDate(dateOfHarvesting)
                                .publishedHarvestDate(Date.from(dateOfHarvesting.toInstant().plus(3, ChronoUnit.DAYS)))
                                .build()
                )));
        assertTrue(categorizationResult.shouldBeDropped());
    }
    @Test
    public void shouldCategorizeRecordAsReadyForProcessingInCaseOfEqualsRecordDatestampAndPublishedHarvestDate() {
        //given
        HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
        Date dateOfHarvesting = new Date();
        when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
                Optional.of(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .publishedHarvestDate(dateOfHarvesting)
                                .build()
                ));
        HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HarvestedRecordCategorizationService(harvestedRecordsDAO);

        //when
        CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
                CategorizationParameters.builder()
                        .datasetId("exampleDatasetId")
                        .recordId("exampleRecordId")
                        .recordDateStamp(dateOfHarvesting.toInstant())
                        .currentHarvestDate(dateOfHarvesting.toInstant())
                        .build());
        //then
        verify(harvestedRecordsDAO, times(1)).findRecord(eq("exampleDatasetId"), eq("exampleRecordId"));
        verify(harvestedRecordsDAO, times(1)).insertHarvestedRecord(
                argThat(samePropertyValuesAs(
                        HarvestedRecord.builder()
                                .metisDatasetId("exampleDatasetId")
                                .recordLocalId("exampleRecordId")
                                .latestHarvestDate(dateOfHarvesting)
                                .publishedHarvestDate(dateOfHarvesting)
                                .build()
                )));
        assertTrue(categorizationResult.shouldBeProcessed());
    }
}