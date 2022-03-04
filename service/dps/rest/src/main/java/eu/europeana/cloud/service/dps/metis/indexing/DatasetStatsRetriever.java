package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves statistics related with Metis dataset.
 */
public class DatasetStatsRetriever extends IndexWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetStatsRetriever.class);

    public long getTotalRecordsForDataset(MetisDataSetParameters metisDataSetParameters) throws IndexingException {
        LOGGER.info("Reading total number of records for {}", metisDataSetParameters);
        return indexers.get(metisDataSetParameters.getTargetIndexingDatabase()).countRecords(metisDataSetParameters.getDataSetId());
    }
}