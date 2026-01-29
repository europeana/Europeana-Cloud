package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.indexing.exception.IndexingException;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remove dataset based on a specific date for indexing topology.
 * <p>
 * Created by pwozniak on 10/2/18
 */
public class DatasetCleaner {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetCleaner.class);
  private final IndexWrapper indexWrapper;
  private final DataSetCleanerParameters cleanerParameters;
  private final TargetIndexingDatabase databaseLocation;

  public DatasetCleaner(IndexWrapper indexWrapper, DataSetCleanerParameters cleanerParameters) {
    this.indexWrapper = indexWrapper;
    this.cleanerParameters = cleanerParameters;
    databaseLocation = TargetIndexingDatabase.valueOf(this.cleanerParameters.getTargetIndexingEnv());
  }

  public int getRecordsCount() throws IndexingException {
    return (int) indexWrapper.getIndexer(databaseLocation)
                             .countRecords(cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
  }

  public Stream<String> getRecordIds() throws IndexingException {
    return indexWrapper.getIndexer(databaseLocation)
                       .getRecordIds(this.cleanerParameters.getDataSetId(), this.cleanerParameters.getCleaningDate());
  }

  public void execute() throws DatasetCleaningException {
    LOGGER.info("Executing initial actions for indexing topology");
    try {
      removeDataSet(cleanerParameters.getDataSetId());
    } catch (IndexingException e) {
      String message = "Dataset was not removed correctly";
      throw new DatasetCleaningException(message, e);
    }
  }

  private void removeDataSet(String datasetId) throws IndexingException {
    LOGGER.info("Removing data set {} from solr and mongo", datasetId);
    indexWrapper.getIndexer(databaseLocation).removeAll(datasetId, cleanerParameters.getCleaningDate());
    LOGGER.info("Data set removed");
  }
}
