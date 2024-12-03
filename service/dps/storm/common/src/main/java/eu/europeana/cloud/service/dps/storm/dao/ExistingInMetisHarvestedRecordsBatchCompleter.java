package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import java.util.Date;
import java.util.UUID;

/**
 *  Completes information in the harvested_records_table, for the records we know that exist in the Metis
 *  indexing database, but could be not current, not synchronized in the harvested_records_table.
 *  The completion is done only based on the recordId passed to the completer, so we should only pass
 *  the records that exists in the Metis db. The completer does the update if:
 *  -the record does not exist in the harvested_records_table - the row is created.
 *  -the record exists in the harvested_records table but have indexing state columns null - the checking
 *  and the completing is done only for one group of columns - preview or publish depending on the parameter
 *  passed to the class constructor.
 *  Because we have not, information that normally is saved only in harvested_records_table we fill the columns
 *  with values that are not strictly actual, but values that make incremental algorithm work properly:
 *  -the harvesting date column is fill with new Date(0)  (1970-01-01)
 *  -the md5 columns is filled with generated random UUID.
 */
public class ExistingInMetisHarvestedRecordsBatchCompleter extends AbstractHarvestedRecordsBatchUpdater {

  /**
   * Creates the completer
   * @param dao - HarvestedRecordsDAO
   * @param metisDatasetId - metis dataset id
   * @param targetDb - Metis indexing database for which the completion is done
   */
  public ExistingInMetisHarvestedRecordsBatchCompleter(HarvestedRecordsDAO dao, String metisDatasetId, TargetIndexingDatabase targetDb) {
    super(dao, metisDatasetId, targetDb);
  }

  protected BoundStatement createRequest(String recordId) {
    return dao.createCompleteIndexedColumnsIfEmptyStatement(metisDatasetId, recordId, targetDb, new Date(0), UUID.randomUUID());
  }

}
