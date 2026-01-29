package eu.europeana.cloud.http.service;

import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationResult;
import eu.europeana.cloud.service.dps.storm.service.HarvestedRecordCategorizationService;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

class HttpTopologyCategorizationServiceTest {

  @Test
  void shouldCategorizeRecordAsReadyForProcessingInCaseOfNoDefinitionInDB() {
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

  static Stream<Arguments> existingRecordCases() {
    UUID A = UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f");
    UUID B = UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc91");

    return Stream.of(
            // differs from preview & publish
            Arguments.of(A, A, B, true),

            // differs from preview only
            Arguments.of(B, A, A, true),

            // differs from publish only
            Arguments.of(A, B, A, true),

            // same as preview & publish → drop
            Arguments.of(A, A, A, false)
    );
  }

  @ParameterizedTest(name = "[{index}] preview={0}, publish={1}, incoming={2}, shouldProcess={3}")
  @MethodSource("existingRecordCases")
  void shouldCategorizeRecordBasedOnExistingDefinition(
          UUID previewMd5,
          UUID publishMd5,
          UUID incomingMd5,
          boolean shouldBeProcessed
  ) {
    // given
    HarvestedRecordsDAO harvestedRecordsDAO = Mockito.mock(HarvestedRecordsDAO.class);

    Instant dateOfHarvesting =
            LocalDateTime.of(1990, 1, 20, 10, 15).toInstant(ZoneOffset.UTC);

    when(harvestedRecordsDAO.findRecord(anyString(), anyString()))
            .thenReturn(Optional.of(
                    HarvestedRecord.builder()
                            .metisDatasetId("exampleDatasetId")
                            .recordLocalId("exampleRecordId")
                            .previewHarvestMd5(previewMd5)
                            .publishedHarvestMd5(publishMd5)
                            .build()
            ));

    HarvestedRecordCategorizationService service =
            new HttpTopologyCategorizationService(harvestedRecordsDAO);

    // when
    CategorizationResult result = service.categorize(
            CategorizationParameters.builder()
                    .datasetId("exampleDatasetId")
                    .recordId("exampleRecordId")
                    .recordMd5(incomingMd5)
                    .currentHarvestDate(dateOfHarvesting)
                    .build()
    );

    // then
    verify(harvestedRecordsDAO).findRecord("exampleDatasetId", "exampleRecordId");
    verify(harvestedRecordsDAO)
            .updateLatestHarvestDateAndMd5(
                    eq("exampleDatasetId"),
                    eq("exampleRecordId"),
                    any(),
                    any()
            );

    if (shouldBeProcessed) {
      assertTrue(result.shouldBeProcessed());
    } else {
      assertTrue(result.shouldBeDropped());
    }
  }
}