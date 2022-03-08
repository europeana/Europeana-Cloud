package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves statistics related with Metis dataset.
 */
public class DatasetStatsRetriever {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetStatsRetriever.class);
    private IndexWrapper indexWrapper;

    public DatasetStatsRetriever(IndexWrapper indexWrapper) {
        this.indexWrapper = indexWrapper;
    }

    public long getTotalRecordsForDataset(MetisDataSetParameters metisDataSetParameters) throws IndexingException {
        LOGGER.info("Reading total number of records for {}", metisDataSetParameters);
        return indexWrapper.getIndexer(metisDataSetParameters.getTargetIndexingDatabase())
                .countRecords(metisDataSetParameters.getDataSetId());
    }
}