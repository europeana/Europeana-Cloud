package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
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
    private final DatabaseLocation databaseLocation;

    public DatasetCleaner(DataSetCleanerParameters cleanerParameters) {
        this.cleanerParameters = cleanerParameters;
        loadProperties();
        databaseLocation = evaluateDatabaseLocation();
    }

    public int getRecordsCount() throws SetupRelatedIndexingException, IndexerRelatedIndexingException {
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
            LOGGER.error("Dataset was not removed correctly. ", e);
            throw new DatasetCleaningException("Dataset was not removed correctly.", e);
        }
    }

    private DatabaseLocation evaluateDatabaseLocation() {
        return evaluateDatabaseLocation(MetisDataSetParameters.builder()
                .dataSetId(cleanerParameters.getDataSetId())
                .targetIndexingDatabase(TargetIndexingDatabase.valueOf(cleanerParameters.getTargetIndexingEnv()))
                .build());
    }

    private void removeDataSet(String datasetId) throws IndexingException {
        LOGGER.info("Removing data set {} from solr and mongo", datasetId);
        indexers.get(databaseLocation).removeAll(datasetId, cleanerParameters.getCleaningDate());
        LOGGER.info("Data set removed");
    }
}