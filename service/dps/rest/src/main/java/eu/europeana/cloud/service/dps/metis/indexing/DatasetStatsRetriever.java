package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum DatabaseLocation {
    DEFAULT_PREVIEW,
    DEFAULT_PUBLISH,
    ALT_PREVIEW,
    ALT_PUBLISH,
}

/**
 * Retrieves statistics related with Metis dataset.
 */
public class DatasetStatsRetriever extends IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetStatsRetriever.class);

    public long getTotalRecordsForDataset(MetisDataSetParameters metisDataSetParameters) throws IndexingException {
        LOGGER.info("Reading total number of records for {}", metisDataSetParameters);
        DatabaseLocation databaseLocation = evaluateDatabaseLocation(metisDataSetParameters);
        return indexers.get(databaseLocation).countRecords(metisDataSetParameters.getDataSetId());
    }
}