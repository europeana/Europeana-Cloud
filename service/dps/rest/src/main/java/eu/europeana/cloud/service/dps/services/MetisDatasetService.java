package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.MetisDataset;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetStatsRetriever;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingEnvironment;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetisDatasetService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetisDatasetService.class);

    private final DatasetStatsRetriever datasetStatsRetriever;

    public MetisDatasetService(DatasetStatsRetriever datasetStatsRetriever){
        this.datasetStatsRetriever = datasetStatsRetriever;
    }

    public MetisDataset prepareStatsFor(MetisDataset metisDataset, TargetIndexingDatabase targetIndexingDatabase, TargetIndexingEnvironment targetIndexingEnvironment) throws IndexingException {
        LOGGER.info("Reading dataset stats for dataset: {}", metisDataset);
        DataSetParameters parameters = new DataSetParameters(metisDataset.getId(), targetIndexingDatabase, targetIndexingEnvironment, null);
        MetisDataset result = MetisDataset.builder()
                .id(metisDataset.getId())
                .elements(datasetStatsRetriever.getTotalRecordsForDataset(parameters))
                .build();
        LOGGER.info("Found stats: {}", result);
        return result;
    }
}
