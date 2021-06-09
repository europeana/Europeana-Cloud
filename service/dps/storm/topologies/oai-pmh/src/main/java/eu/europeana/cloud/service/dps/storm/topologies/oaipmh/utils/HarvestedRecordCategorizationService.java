package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils;

import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;

/**
 * Service responsible for categorization of the records in the incremental harvesting.
 * Decision will be taken based on the record identifier (usually rdf:about) and record datestamp (taken from the oai).
 */
public class HarvestedRecordCategorizationService {

    private static final int DATE_BUFFER_IN_MINUTES = 60 * 24 * 2;
    private final HarvestedRecordsDAO harvestedRecordsDAO;

    public HarvestedRecordCategorizationService(HarvestedRecordsDAO harvestedRecordsDAO) {
        this.harvestedRecordsDAO = harvestedRecordsDAO;
    }

    public CategorizationResult categorize(CategorizationParameters categorizationParameters) {
        Optional<HarvestedRecord> harvestedRecord = readRecordFromDB(categorizationParameters);
        if (harvestedRecord.isEmpty()) {
            var newHarvestedRecord = prepareHarvestedRecordDefinition(categorizationParameters);
            addRecordDefinitionToDB(newHarvestedRecord);
            return categorizeRecordAsReadyForProcessing(categorizationParameters, harvestedRecord);
        } else {
            updateRecordLatestHarvestDate(harvestedRecord.get(), categorizationParameters.getCurrentHarvestDate());
            if (categorizationParameters.isFullHarvest()
                    || recordDateStampOlderThanPreviewVersion(categorizationParameters.getRecordDateStamp(), harvestedRecord.get())
                    || recordDateStampOlderThanPublishedVersion(categorizationParameters.getRecordDateStamp(), harvestedRecord.get())
                    ) {
                return categorizeRecordAsReadyForProcessing(categorizationParameters, harvestedRecord);
            }
            return categorizeRecordAsNotReadyForProcessing(categorizationParameters, harvestedRecord);
        }
    }

    private Optional<HarvestedRecord> readRecordFromDB(CategorizationParameters categorizationParameters) {
        return harvestedRecordsDAO.findRecord(
                categorizationParameters.getDatasetId(),
                categorizationParameters.getRecordId());
    }

    private HarvestedRecord prepareHarvestedRecordDefinition(CategorizationParameters categorizationParameters) {
        return HarvestedRecord
                .builder()
                .metisDatasetId(categorizationParameters.getDatasetId())
                .recordLocalId(categorizationParameters.getRecordId())
                .latestHarvestDate(Date.from(categorizationParameters.getCurrentHarvestDate()))
                .build();
    }

    private void addRecordDefinitionToDB(HarvestedRecord harvestedRecord) {
        harvestedRecordsDAO.insertHarvestedRecord(harvestedRecord);
    }

    private void updateRecordLatestHarvestDate(HarvestedRecord harvestedRecord, Instant currentHarvestDate) {
        harvestedRecord.setLatestHarvestDate(Date.from(currentHarvestDate));
        harvestedRecordsDAO.updateLatestHarvestDate(
                harvestedRecord.getMetisDatasetId(),
                harvestedRecord.getRecordLocalId(),
                harvestedRecord.getLatestHarvestDate());
    }

    private boolean recordDateStampOlderThanPreviewVersion(Instant recordDateStamp, HarvestedRecord harvestedRecord) {
        return harvestedRecord.getPreviewHarvestDate() == null
                ||
        recordDateStamp.plus(DATE_BUFFER_IN_MINUTES, ChronoUnit.MINUTES).isAfter(harvestedRecord.getPreviewHarvestDate().toInstant());
    }

    private boolean recordDateStampOlderThanPublishedVersion(Instant recordDateStamp, HarvestedRecord harvestedRecord) {
        return harvestedRecord.getPublishedHarvestDate() == null
                ||
                recordDateStamp.plus(DATE_BUFFER_IN_MINUTES, ChronoUnit.MINUTES).isAfter(harvestedRecord.getPublishedHarvestDate().toInstant());
    }


    private CategorizationResult categorizeRecordAsReadyForProcessing(CategorizationParameters categorizationParameters, Optional<HarvestedRecord> harvestedRecord) {
        return CategorizationResult
                .builder()
                .category(CategorizationResult.Category.ELIGIBLE_FOR_PROCESSING)
                .categorizationParameters(categorizationParameters)
                .harvestedRecord(harvestedRecord.orElse(null))
                .build();
    }

    private CategorizationResult categorizeRecordAsNotReadyForProcessing(CategorizationParameters categorizationParameters, Optional<HarvestedRecord> harvestedRecord) {
        return CategorizationResult
                .builder()
                .category(CategorizationResult.Category.ALREADY_PROCESSED)
                .categorizationParameters(categorizationParameters)
                .harvestedRecord(harvestedRecord.orElse(null))
                .build();
    }
}
