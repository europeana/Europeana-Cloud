package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;

/**
 *  Cleans indexed date and md5 columns for selected environment in the harvested_records_table.
 *  (columns preview_harvest_date and preview_harvest_md5 for preview environment or published_harvest_date
 *  and published_harvest_md5  for publish environment).
 */
public class HarvestedRecordsBatchCleaner extends AbstractHarvestedRecordsBatchUpdater {

  /**
   * Creates HarvestedRecordsBatchCleaner
   * @param dao - HarvestedRecordsDAO
   * @param metisDatasetId - metis dataset id
   * @param targetDb - Metis indexing database for which the completion is done
   */
  public HarvestedRecordsBatchCleaner(HarvestedRecordsDAO dao, String metisDatasetId, TargetIndexingDatabase targetDb) {
    super(dao, metisDatasetId, targetDb);
  }

  protected BoundStatement createRequest(String recordId) {
    return dao.createCleanIndexedColumnsStatement(metisDatasetId, recordId, targetDb);
  }

}
