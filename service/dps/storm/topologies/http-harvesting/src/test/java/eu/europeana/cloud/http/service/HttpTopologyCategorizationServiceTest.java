package eu.europeana.cloud.http.service;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationResult;
import eu.europeana.cloud.service.dps.storm.service.HarvestedRecordCategorizationService;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.mockito.Mockito;

public class HttpTopologyCategorizationServiceTest {

  @Test
  public void shouldCategorizeRecordAsReadyForProcessingInCaseOfNoDefinitionInDB() {
    //given
    HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
    HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HttpTopologyCategorizationService(
        harvestedRecordsDAO);

    Instant dateOfHarvesting =
        LocalDateTime.of(1990, 1, 19, 10, 15).toInstant(ZoneOffset.UTC);

    //when
    CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
        CategorizationParameters.builder()
                                .datasetId("exampleDatasetId")
                                .recordId("exampleRecordId")
                                .recordMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                                .currentHarvestDate(dateOfHarvesting)
                                .build());
    //then
    verify(harvestedRecordsDAO, times(1)).findRecord("exampleDatasetId", "exampleRecordId");
    verify(harvestedRecordsDAO, times(1)).insertHarvestedRecord(
        argThat(samePropertyValuesAs(
            HarvestedRecord.builder()
                           .metisDatasetId("exampleDatasetId")
                           .recordLocalId("exampleRecordId")
                           .latestHarvestDate(Date.from(dateOfHarvesting))
                           .latestHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                           .build()
        )));
    assertTrue(categorizationResult.shouldBeProcessed());
  }

  @Test
  public void shouldCategorizeRecordAsReadyForProcessingInCaseOfExistingDefinitionInDBAndMd5ThatDiffersFromPreviewAndPublish() {
    //given
    HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);

    Instant dateOfHarvesting =
        LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC);

    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
        Optional.of(
            HarvestedRecord.builder()
                           .metisDatasetId("exampleDatasetId")
                           .recordLocalId("exampleRecordId")
                           .previewHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                           .publishedHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                           .build()
        ));
    HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HttpTopologyCategorizationService(
        harvestedRecordsDAO);

    //when
    CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
        CategorizationParameters.builder()
                                .datasetId("exampleDatasetId")
                                .recordId("exampleRecordId")
                                .recordMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc91"))
                                .currentHarvestDate(dateOfHarvesting)
                                .build());
    //then
    verify(harvestedRecordsDAO, times(1)).findRecord("exampleDatasetId", "exampleRecordId");
    verify(harvestedRecordsDAO, times(1))
        .updateLatestHarvestDateAndMd5(eq("exampleDatasetId"), eq("exampleRecordId"), any(), any());
    assertTrue(categorizationResult.shouldBeProcessed());
  }

  @Test
  public void shouldCategorizeRecordAsReadyForProcessingInCaseOfExistingDefinitionInDBAndMd5ThatDiffersFromPreview() {
    //given
    HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);

    Instant dateOfHarvesting =
        LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC);

    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
        Optional.of(
            HarvestedRecord.builder()
                           .metisDatasetId("exampleDatasetId")
                           .recordLocalId("exampleRecordId")
                           .previewHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc91"))
                           .publishedHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                           .build()
        ));
    HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HttpTopologyCategorizationService(
        harvestedRecordsDAO);

    //when
    CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
        CategorizationParameters.builder()
                                .datasetId("exampleDatasetId")
                                .recordId("exampleRecordId")
                                .recordMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                                .currentHarvestDate(dateOfHarvesting)
                                .build());
    //then
    verify(harvestedRecordsDAO, times(1)).findRecord("exampleDatasetId", "exampleRecordId");
    verify(harvestedRecordsDAO, times(1))
        .updateLatestHarvestDateAndMd5(eq("exampleDatasetId"), eq("exampleRecordId"), any(), any());
    assertTrue(categorizationResult.shouldBeProcessed());
  }

  @Test
  public void shouldCategorizeRecordAsReadyForProcessingInCaseOfExistingDefinitionInDBAndMd5ThatDiffersFromPublish() {
    //given
    HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);

    Instant dateOfHarvesting =
        LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC);

    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
        Optional.of(
            HarvestedRecord.builder()
                           .metisDatasetId("exampleDatasetId")
                           .recordLocalId("exampleRecordId")
                           .previewHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc91"))
                           .publishedHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                           .build()
        ));
    HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HttpTopologyCategorizationService(
        harvestedRecordsDAO);

    //when
    CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
        CategorizationParameters.builder()
                                .datasetId("exampleDatasetId")
                                .recordId("exampleRecordId")
                                .recordMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc91"))
                                .currentHarvestDate(dateOfHarvesting)
                                .build());
    //then
    verify(harvestedRecordsDAO, times(1)).findRecord("exampleDatasetId", "exampleRecordId");
    verify(harvestedRecordsDAO, times(1))
        .updateLatestHarvestDateAndMd5(eq("exampleDatasetId"), eq("exampleRecordId"), any(), any());
    assertTrue(categorizationResult.shouldBeProcessed());
  }

  @Test
  public void shouldCategorizeRecordAsNotReadyForProcessingInCaseOfExistingDefinitionInDBAndMd5SameAsForPublishAndPreview() {
    //given
    HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);
    Instant dateOfHarvesting =
        LocalDateTime.of(1990, 1, 25, 10, 15).toInstant(ZoneOffset.UTC);
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(
        Optional.of(
            HarvestedRecord.builder()
                           .metisDatasetId("exampleDatasetId")
                           .recordLocalId("exampleRecordId")
                           .previewHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                           .publishedHarvestMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                           .build()
        ));
    HarvestedRecordCategorizationService harvestedRecordCategorizationService = new HttpTopologyCategorizationService(
        harvestedRecordsDAO);

    //when
    CategorizationResult categorizationResult = harvestedRecordCategorizationService.categorize(
        CategorizationParameters.builder()
                                .datasetId("exampleDatasetId")
                                .recordId("exampleRecordId")
                                .recordMd5(UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f"))
                                .currentHarvestDate(dateOfHarvesting)
                                .build());
    //then
    verify(harvestedRecordsDAO, times(1)).findRecord("exampleDatasetId", "exampleRecordId");
    verify(harvestedRecordsDAO, times(1))
        .updateLatestHarvestDateAndMd5(eq("exampleDatasetId"), eq("exampleRecordId"), any(), any());
    assertTrue(categorizationResult.shouldBeDropped());
  }
}