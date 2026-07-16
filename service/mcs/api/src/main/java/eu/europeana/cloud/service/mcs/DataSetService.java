package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.*;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Service for data sets and representation assignments to data sets.
 */
public interface DataSetService {

  /**
   * Returns all representations from particular data set (in slices). If data set contains representation not in specified
   * version, the latest persistent representation version will be returned.
   *
   * @param providerId provider's (owner of data set) id.
   * @param dataSetId data set id
   * @param thresholdParam if null - will return first result slice. Result slices contain token for next pages, which should be
   * provided in this parameter.
   * @param limit max number of results in one slice.
   * @param existingOnly returns only representations that contain files if set to true
   * @return list of representations as a result slice.
   * @throws DataSetNotExistsException dataset not exists.
   */
  ResultSlice<Representation> listDataSet(String providerId, String dataSetId,
                                          String thresholdParam, boolean existingOnly, int limit) throws DataSetNotExistsException;

  void addAssignmentToMainTables(String providerId, String dataSetId, String recordId, String schema, String version, boolean markDepublished);

  /**
   * Removes representation assignment from data set.
   *
   * @param providerId owner of data set
   * @param dataSetId data set id
   * @param recordId id of record
   * @param schema schema name of representation
   * @param versionId version id of representation
   * @throws DataSetNotExistsException if such data set not exists
   */
  void removeAssignment(String providerId, String dataSetId, String recordId, String schema, String versionId)
      throws DataSetNotExistsException;


  /**
   * Creates a new data set for specified provider.
   *
   * @param providerId owner of data set
   * @param dataSetId identifier of newly created data set
   * @param description description of newly created data set (may be any text)
   * @return created data set.
   * @throws ProviderNotExistsException no such data provider exists
   * @throws DataSetAlreadyExistsException data set with this identifer has already been created for this provider
   */
  DataSet createDataSet(String providerId, String dataSetId, String description)
      throws ProviderNotExistsException, DataSetAlreadyExistsException;


  /**
   * Updates description of data set.
   *
   * @param providerId owner of data set
   * @param dataSetId identifier of newly created data set
   * @param description new description of data set (may be any text)
   * @return updated data set
   * @throws DataSetNotExistsException no such data set exists
   */
  DataSet updateDataSet(String providerId, String dataSetId, String description)
      throws DataSetNotExistsException;


  /**
   * Returns all data sets for particular data provider (in slices).
   *
   * @param providerId provider id.
   * @param thresholdDatasetId if null - will return first result slice. Result slices contain token for next pages, which should
   * be provided in this parameter.
   * @param limit max number of results in one slice.
   * @return list of data sets as a result slice.
   */
  ResultSlice<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit);

  /**
   * Deletes data set.
   *
   * @param providerId provider id
   * @param dataSetId data set id.
   * @throws DataSetNotExistsException no such data set exists (data provider does not have one or data provider not exist)
   */
  void deleteDataSet(String providerId, String dataSetId)
      throws DataSetDeletionException, DataSetNotExistsException;

 /**
   * Returns one (usually the first one from DB) for the given representation
   *
   * @param cloudId cloud identifier to be used
   * @param representationName representation name to be used
   * @param version version of representation that we get dataset for
   * @return found data set
   * @throws RepresentationNotExistsException in case of non-existing representation version
   */
  Optional<CompoundDataSetId> getOneDatasetFor(String cloudId, String representationName, UUID version) throws RepresentationNotExistsException;

  void checkIfDatasetExists(String dataSetId, String providerId) throws DataSetNotExistsException;
}
