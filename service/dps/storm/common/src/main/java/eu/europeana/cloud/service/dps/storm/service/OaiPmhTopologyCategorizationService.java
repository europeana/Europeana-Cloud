package eu.europeana.cloud.service.dps.storm.service;

import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class OaiPmhTopologyCategorizationService extends HarvestedRecordCategorizationService {

    private static final int DATE_BUFFER_IN_MINUTES = 60 * 24 * 2;

    public OaiPmhTopologyCategorizationService(HarvestedRecordsDAO harvestedRecordsDAO) {
        super(harvestedRecordsDAO);
    }

    @Override
    protected boolean isRecordEligibleForProcessing(HarvestedRecord harvestedRecord, CategorizationParameters categorizationParameters) {
        return categorizationParameters.isFullHarvest()
                || recordDateStampOlderThanPreviewVersion(categorizationParameters.getRecordDateStamp(), harvestedRecord)
                || recordDateStampOlderThanPublishedVersion(categorizationParameters.getRecordDateStamp(), harvestedRecord);
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
}
