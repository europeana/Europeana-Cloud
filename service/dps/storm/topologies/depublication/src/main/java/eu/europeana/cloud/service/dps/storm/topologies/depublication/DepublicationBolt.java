package eu.europeana.cloud.service.dps.storm.topologies.depublication;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexedRecordRemover;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.utils.DepublicationReason;
import java.util.Properties;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DepublicationBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationBolt.class);
  private final Properties indexingProperties;
  private transient HarvestedRecordsDAO harvestedRecordsDAO;
  private transient IndexedRecordRemover recordRemover;

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

      boolean removedSuccessfully =
          recordRemover.removeRecord(TargetIndexingDatabase.PUBLISH, recordEuropeanaId, depublicationReason);

      if (removedSuccessfully) {
        cleanRecordInHarvestedRecordsTable(metisDatasetId, recordEuropeanaId);
        emitSuccessNotification(anchorTuple, stormTaskTuple);
        LOGGER.info("The the record: {} successfully depublished, because of: {}.", recordEuropeanaId, depublicationReason);
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

  @Override
  public void prepare() {
    prepareDao();
    IndexWrapper indexWrapper = IndexWrapper.getInstance(indexingProperties);
    recordRemover = new IndexedRecordRemover(indexWrapper);
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

  private void cleanRecordInHarvestedRecordsTable(String metisDatasetId, String recordId) {
    harvestedRecordsDAO.findRecord(metisDatasetId, recordId).ifPresent(this::cleanRecordInHarvestedRecordsTable);
  }

  private void cleanRecordInHarvestedRecordsTable(HarvestedRecord theRecord) {
    theRecord.setPublishedHarvestDate(null);
    theRecord.setPublishedHarvestMd5(null);
    harvestedRecordsDAO.insertHarvestedRecord(theRecord);
  }

}
