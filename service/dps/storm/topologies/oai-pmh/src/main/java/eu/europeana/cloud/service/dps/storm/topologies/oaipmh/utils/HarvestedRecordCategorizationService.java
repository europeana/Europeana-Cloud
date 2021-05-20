package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils;

import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordsDAO;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

/**
 * Service responsible for categorization of the records in the incremental harvesting.
 * Decision will be taken based on the record identifier (usually rdf:about) and record datestamp (taken from the oai).
 */
public class HarvestedRecordCategorizationService {

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
            updateRecordDefinitionInDB(harvestedRecord.get());
            if (recordDateStampOlderThanPublishedVersion(categorizationParameters.getRecordDateStamp(), harvestedRecord.get())) {
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
    }

    private boolean recordDateStampOlderThanPublishedVersion(Instant recordDateStamp, HarvestedRecord harvestedRecord) {

        return harvestedRecord.getPublishedHarvestDate() == null
                ||
                harvestedRecord.getPublishedHarvestDate().toInstant().isBefore(recordDateStamp);
    }

    private void updateRecordDefinitionInDB(HarvestedRecord harvestedRecord) {
        harvestedRecordsDAO.insertHarvestedRecord(harvestedRecord);
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
