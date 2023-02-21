package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils;

import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.service.HarvestedRecordCategorizationService;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * <p>Contains the categorization algorithm for the full OAI harvesting. It based on the OAI dateStamp and published and preview
 * harvested dates. To further understand why the {@link #DATE_BUFFER_IN_MINUTES} has a value of minutes equivalent to 2
 * days(2880), here is an example:</p>
 *
 * <p>Some repositories do not have time support and would always return a rounded down date.<br/>
 * In our example a record from repository has response dateStamp 2021-01-01T00:00:00Z<br/> The record from the server can at any
 * time on that date be updated but the response will always have the same rounded down dateStamp value.</p>
 *
 * <p>First time full harvest at server date 2021-01-01T23:55:00Z<br/>
 * 2021-01-01T00:00:00Z (New record)</p><br/>
 *
 * <p>First time incremental at server date 2021-01-02T00:05:00Z:<br/>
 * 2021-01-01T00:00:00Z + 2880m = 2021-01-03T00:00:00Z  -><br/> 2021-01-03T00:00:00Z > 2021-01-01T23:55:00Z (Update
 * record)</p><br/>
 *
 * <p>Second time incremental at server date 2021-01-02T00:10:00Z:<br/>
 * 2021-01-01T00:00:00Z + 2880m = 2021-01-03T00:00:00Z -><br/> 2021-01-03T00:00:00Z > 2021-01-02T00:05:00Z (Update
 * record)</p><br/>
 *
 * <p>Third time incremental at server date 2021-01-03T00:15:00Z:<br/>
 * 2021-01-01T00:00:00Z + 2880m = 2021-01-03T00:00:00Z -><br/> 2021-01-03T00:00:00Z > 2021-01-02T00:10:00Z (Update
 * record)</p><br/>
 *
 * <p>Fourth time incremental at server date 2021-01-03T00:20:00Z:<br/>
 * 2021-01-01T00:00:00Z + 2880m = 2021-01-03T00:00:00Z -><br/> 2021-01-03T00:00:00Z < 2021-01-03T00:15:00Z (Ignore
 * record)</p><br/>
 *
 * <p>As you can see on the third incremental harvest we still had to process the record because the stored date for the record
 * was 2021-01-02T00:10:00Z, and only after that, on the fourth time onward, the record will have surpassed the buffer and we
 * would ignore it.</p>
 */
public class OaiPmhTopologyCategorizationService extends HarvestedRecordCategorizationService {

  private static final int DATE_BUFFER_IN_MINUTES = 60 * 24 * 2;

  public OaiPmhTopologyCategorizationService(HarvestedRecordsDAO harvestedRecordsDAO) {
    super(harvestedRecordsDAO);
  }

  @Override
  protected boolean isRecordEligibleForProcessing(HarvestedRecord harvestedRecord,
      CategorizationParameters categorizationParameters) {
    return categorizationParameters.isFullHarvest()
        || previewVersionIsOlderThanRecordDateStamp(categorizationParameters.getRecordDateStamp(), harvestedRecord)
        || publishedVersionIsOlderThanRecordDateStamp(categorizationParameters.getRecordDateStamp(), harvestedRecord);
  }

  private boolean previewVersionIsOlderThanRecordDateStamp(Instant recordDateStamp, HarvestedRecord harvestedRecord) {
    return harvestedRecord.getPreviewHarvestDate() == null
        ||
        recordDateStamp.plus(DATE_BUFFER_IN_MINUTES, ChronoUnit.MINUTES)
                       .isAfter(harvestedRecord.getPreviewHarvestDate().toInstant());
  }

  private boolean publishedVersionIsOlderThanRecordDateStamp(Instant recordDateStamp, HarvestedRecord harvestedRecord) {
    return harvestedRecord.getPublishedHarvestDate() == null
        ||
        recordDateStamp.plus(DATE_BUFFER_IN_MINUTES, ChronoUnit.MINUTES)
                       .isAfter(harvestedRecord.getPublishedHarvestDate().toInstant());
  }
}
