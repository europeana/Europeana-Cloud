package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import java.util.Date;
import java.util.UUID;

/**
 *  Completes information in the <b>harvested_records_table</b>, for the records we know that exist in the Metis
 *  indexing database, but could be not current, not synchronized in the <b>harvested_records</b> table.
 *  The completion is done only based on the record id passed to the completer, so we should only pass
 *  the records that really exists in the Metis db. The completer does the update if:
 *  <ul>
 *  <li>the record does not exist in the <b>harvested_records</b> table - the row is created.</li>
 *  <li>the record exists in the <b>harvested_records</b> table but have indexing state columns null - the checking
 *  and the completing is done only for one group of columns - preview or publish depending on the parameter
 *  passed to the class constructor.</li>
 *  </ul>
 *  Because we have not, information that normally is saved only in <b>harvested_records</b> table we fill the columns
 *  with values that are not strictly actual, but values that make incremental algorithm work properly:
 *  <ul>
 *  <li>the harvesting date column is fill with new Date(0)  (1970-01-01)</li>
 *  <li>the md5 columns is filled with generated random UUID</li>
 *  </ul>
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
    return dao.createUpdateIndexedColumnsIfEmptyHarvestDateStatement(metisDatasetId, recordId, targetDb, new Date(0), UUID.randomUUID());
  }

}
