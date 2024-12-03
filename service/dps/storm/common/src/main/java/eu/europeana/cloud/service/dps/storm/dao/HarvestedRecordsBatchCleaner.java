package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;

public class HarvestedRecordsBatchCleaner extends AbstractHarvestedRecordsBatchUpdater {

  public HarvestedRecordsBatchCleaner(HarvestedRecordsDAO dao, String metisDatasetId, TargetIndexingDatabase targetDb) {
    super(dao, metisDatasetId, targetDb);
  }

  protected BoundStatement createRequest(String recordId) {
    return dao.createCleanIndexedColumnsStatement(metisDatasetId, recordId, targetDb);
  }

}
