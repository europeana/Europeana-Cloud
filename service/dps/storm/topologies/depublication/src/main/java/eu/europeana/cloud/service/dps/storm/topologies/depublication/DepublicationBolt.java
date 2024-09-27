package eu.europeana.cloud.service.dps.storm.topologies.depublication;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.utils.DepublicationReason;
import java.util.Properties;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DepublicationBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationBolt.class);
  private final Properties indexingProperties;
  private HarvestedRecordsDAO harvestedRecordsDAO;
  private Indexer indexer;

  public DepublicationBolt(CassandraProperties cassandraProperties, Properties indexingProperties) {
    super(cassandraProperties);
    this.indexingProperties = indexingProperties;
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    String recordEuropeanaId = stormTaskTuple.getFileUrl();
    LOGGER.debug("Depublishing the record: {} ...", recordEuropeanaId);
    try {
      String metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
      DepublicationReason depublicationReason = DepublicationReason.valueOf(
          stormTaskTuple.getParameter(PluginParameterKeys.DEPUBLICATION_REASON));

      boolean removedSuccessfully = removeRecord(recordEuropeanaId, depublicationReason);

      if (removedSuccessfully) {
        cleanRecordInHarvestedRecordsTable(metisDatasetId, recordEuropeanaId);
        emitSuccessNotification(anchorTuple, stormTaskTuple);
        LOGGER.info("The the record: {} successfully depublished.", recordEuropeanaId);
      } else {
        emitErrorNotification(anchorTuple, stormTaskTuple, "Record could not be depublished!",
            "Could not find the record: " + recordEuropeanaId);
        LOGGER.warn("The the record: {} did not depublished, cause it was not found!", recordEuropeanaId);

      }

    } catch (IndexingException e) {
      emitErrorNotification(anchorTuple, stormTaskTuple, "Exception during record depublishing", e);
    }

    outputCollector.ack(anchorTuple);
  }

  /**
   * Creates tombstone for the record and removes it from the index. The method is as a whole idempotent, so we could execute it
   * multiple time, and we always have the same final results. But this method is not atomic.
   *
   * @param recordId record identifier to be uses for removal
   * @return if removal was successful
   * @throws IndexingException thrown in case of some issues with the removal
   */
  public boolean removeRecord(String recordId, DepublicationReason reason) throws IndexingException {
    boolean recordWasTombstoned = indexer.indexTombstone(recordId, reason);

    if (recordWasTombstoned) {
      return indexer.remove(recordId);
    } else {
      boolean tombstoneExists = indexer.getTombstone(recordId) != null;

      if (tombstoneExists) {
        //If tombstone exists, it means that it was created during previous execution of the method
        // in this case we need to continue removing, because we don't know if it was completed
        // during previous execution.
        indexer.remove(recordId);
        //We always return true cause even if remove returned false it could be removed during
        //previous try, and because we checked the tombstone exists, all is ok.
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public void prepare() {
    prepareDao();
    prepareIndexer();
  }

  private void prepareDao() {
    var cassandraConnectionProvider =
        CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
            cassandraProperties.getHosts(),
            cassandraProperties.getPort(),
            cassandraProperties.getKeyspace(),
            cassandraProperties.getUser(),
            cassandraProperties.getPassword());
    harvestedRecordsDAO = HarvestedRecordsDAO.getInstance(cassandraConnectionProvider);
  }

  private void prepareIndexer() {
    IndexWrapper indexWrapper = IndexWrapper.getInstance(indexingProperties);
    indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH);
  }

  private void cleanRecordInHarvestedRecordsTable(String metisDatasetId, String recordId) {
    harvestedRecordsDAO.findRecord(metisDatasetId, recordId).ifPresent(this::cleanRecordInHarvestedRecordsTable);
  }

  private void cleanRecordInHarvestedRecordsTable(HarvestedRecord theRecord) {
    theRecord.setPublishedHarvestDate(null);
    theRecord.setPublishedHarvestMd5(null);
    harvestedRecordsDAO.insertHarvestedRecord(theRecord);
  }

}
