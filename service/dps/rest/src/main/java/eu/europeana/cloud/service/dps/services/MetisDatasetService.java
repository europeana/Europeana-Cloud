package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.MetisDataset;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetStatsRetriever;
import eu.europeana.cloud.service.dps.metis.indexing.MetisDataSetParameters;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.indexing.exception.IndexingException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetisDatasetService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetisDatasetService.class);

  private final DatasetStatsRetriever datasetStatsRetriever;

  private final HarvestedRecordsDAO harvestedRecordsDAO;

  public MetisDatasetService(DatasetStatsRetriever datasetStatsRetriever, HarvestedRecordsDAO harvestedRecordsDAO) {
    this.datasetStatsRetriever = datasetStatsRetriever;
    this.harvestedRecordsDAO = harvestedRecordsDAO;
  }

  public MetisDataset prepareStatsFor(MetisDataset metisDataset, TargetIndexingDatabase targetIndexingDatabase)
      throws IndexingException {
    LOGGER.info("Reading dataset stats for dataset: {}", metisDataset);
    MetisDataSetParameters parameters = new MetisDataSetParameters(metisDataset.getId(), targetIndexingDatabase, null);
    MetisDataset result = MetisDataset.builder()
                                      .id(metisDataset.getId())
                                      .size(datasetStatsRetriever.getTotalRecordsForDataset(parameters))
                                      .build();
    LOGGER.info("Found stats: {}", result);
    return result;
  }

  /**
   * Returns list of {@link HarvestedRecord} from the given dataset that are published and are specified on recordIdentifiers
   * list.
   *
   * @param metisDataset identifier of Metis dataset that will be searched
   * @param recordIdentifiers list of identifiers that will be used for the filtering
   * @return list of found records
   */
  public List<HarvestedRecord> findPublishedRecordsInSet(MetisDataset metisDataset, List<String> recordIdentifiers) {
    LOGGER.debug("Searching for published record identifiers in {} dataset and the following subset {}", metisDataset.getId(),
        recordIdentifiers);
    List<HarvestedRecord> foundHarvestedRecords = recordIdentifiers
        .stream()
        .map(recordIdentifier -> harvestedRecordsDAO.findRecord(metisDataset.getId(), recordIdentifier))
        .flatMap(Optional::stream)
        .filter(aRecord -> aRecord.getPublishedHarvestDate() != null)
        .toList();
    LOGGER.debug("Found the following record identifiers in {} dataset: {}", metisDataset.getId(), foundHarvestedRecords);
    return foundHarvestedRecords;
  }
}
