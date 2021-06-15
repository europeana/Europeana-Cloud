package eu.europeana.cloud.service.dps.storm.service;

import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;

public class HttpTopologyCategorizationService extends HarvestedRecordCategorizationService {


    public HttpTopologyCategorizationService(HarvestedRecordsDAO harvestedRecordsDAO) {
        super(harvestedRecordsDAO);
    }

    @Override
    protected boolean isRecordEligibleForProcessing(HarvestedRecord harvestedRecord, CategorizationParameters categorizationParameters) {
        return categorizationParameters.isFullHarvest()
                || hashesMismatchForPreviewEnv(harvestedRecord, categorizationParameters)
                || hashesMismatchForPublishEnv(harvestedRecord, categorizationParameters);
    }

    private boolean hashesMismatchForPreviewEnv(HarvestedRecord harvestedRecord, CategorizationParameters categorizationParameters) {
        if (harvestedRecord.getPreviewHarvestMd5() != null) {
            return !categorizationParameters.getRecordMd5().equals(harvestedRecord.getPreviewHarvestMd5());
        } else {
            return true;
        }
    }

    private boolean hashesMismatchForPublishEnv(HarvestedRecord harvestedRecord, CategorizationParameters categorizationParameters) {
        if (harvestedRecord.getPreviewHarvestMd5() != null) {
            return !categorizationParameters.getRecordMd5().equals(harvestedRecord.getPublishedHarvestMd5());
        } else {
            return true;
        }
    }
}
