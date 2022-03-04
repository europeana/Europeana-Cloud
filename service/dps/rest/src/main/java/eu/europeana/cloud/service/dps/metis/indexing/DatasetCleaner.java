package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Remove dataset based on a specific date for indexing topology.
 * <p>
 * Created by pwozniak on 10/2/18
 */
public class DatasetCleaner extends IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetCleaner.class);
    private final DataSetCleanerParameters cleanerParameters;
    private final TargetIndexingDatabase databaseLocation;

    public DatasetCleaner(DataSetCleanerParameters cleanerParameters) {
        this.cleanerParameters = cleanerParameters;
        loadProperties();
        databaseLocation = TargetIndexingDatabase.valueOf(this.cleanerParameters.getTargetIndexingEnv());
    }

    public int getRecordsCount() {
        return (int) indexers
                .get(databaseLocation)
                .countRecords(cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
    }

    public Stream<String> getRecordIds() {
        return indexers.get(databaseLocation).getRecordIds(this.cleanerParameters.getDataSetId(),
                this.cleanerParameters.getCleaningDate());
    }

    public void execute() throws DatasetCleaningException {
        LOGGER.info("Executing initial actions for indexing topology");
        if (properties.isEmpty()) {
            return;
        }
        try {
            removeDataSet(cleanerParameters.getDataSetId());
        } catch (IndexingException e) {
            String message = "Dataset was not removed correctly";
            LOGGER.error(message, e);
            throw new DatasetCleaningException(message, e);
        }
    }

    private void removeDataSet(String datasetId) throws IndexingException {
        LOGGER.info("Removing data set {} from solr and mongo", datasetId);
        indexers.get(databaseLocation).removeAll(datasetId, cleanerParameters.getCleaningDate());
        LOGGER.info("Data set removed");
    }
}