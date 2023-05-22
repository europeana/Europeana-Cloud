package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetDeletionException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;

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
   * @return list of representations as a result slice.
   * @throws DataSetNotExistsException dataset not exists.
   */
  ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit)
      throws DataSetNotExistsException;


  /**
   * Assigns a representation in predefined or latest version to a data set. Temporary representation may be added to a data set
   * only if version is provided. If the same representation was already assigned to a data set, version of representation will be
   * overwritten by provided in this method.
   *
   * @param providerId owner of data set
   * @param dataSetId data set id
   * @param recordId id of record
   * @param schema schema name of representation
   * @param version version of representatnion (if null, the latest persistent version is assigned to a data set)
   * @throws DataSetNotExistsException if such data set not exists
   * @throws RepresentationNotExistsException if such representation does not exist. May be also thrown if version is not provided
   * and no persistent representation version exist for specified schema and record.
   */
  void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
      throws DataSetNotExistsException, RepresentationNotExistsException;

  void addDataSetsRevision(String providerId, String datasetId, Revision revision, String representationName, String cloudId, String version_id);

  void addAssignmentToMainTables(String providerId, String dataSetId, String recordId, String schema, String version);

  /**
   * Removes representation assignment from data set.
   *
   * @param providerId owner of data set
   * @param dataSetId data set id
   * @param recordId id of record
   * @param schema schema name of representation
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
   * @throws DataSetNotExistsException
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
   * Lists all cloudId that are included in given dataSet for given revisionId and representationName.
   *
   * @param providerId dataSet owner
   * @param dataSetId dataSet id
   * @param revisionProviderId revision provider id
   * @param revisionName revision name
   * @param revisionTimestamp revision timestamp
   * @param representationName representation name
   * @param startFrom if null - will return first result slice. Result slices contain token for next pages, which should be
   * provided in this parameter.
   * @param limit max number of results in one slice.
   * @return list of cloudIds and tags in given dataSet for given revisionId and representationName.
   */
  ResultSlice<CloudTagsResponse> getDataSetsRevisions(String providerId, String dataSetId, String revisionProviderId,
      String revisionName, Date revisionTimestamp,
      String representationName, String startFrom, int limit)
      throws ProviderNotExistsException, DataSetNotExistsException;

  /**
   * Lists all cloudId that are included in given dataSet for given revisionId and representationName.
   *
   * @param providerId dataSet owner
   * @param dataSetId dataSet id
   * @param revisionProviderId revision provider id
   * @param revisionName revision name
   * @param revisionTimestamp revision timestamp
   * @param representationName representation name
   * @param limit max number of results in one slice.
   * @return List of cloudIds and tags in given dataSet for given revisionId and representationName.
   */
  List<CloudTagsResponse> getDataSetsExistingRevisions(String providerId, String dataSetId, String revisionProviderId,
      String revisionName, Date revisionTimestamp,
      String representationName, int limit)
      throws ProviderNotExistsException, DataSetNotExistsException;

  /**
   * Remove a revision
   *
   * @param cloudId cloud Id
   * @param representationName representation name
   * @param version representation version
   * @param revisionName revision name
   * @param revisionProviderId revision provider
   * @param revisionTimestamp revision timestamp
   * @throws ProviderNotExistsException
   * @throws RepresentationNotExistsException
   */
  void deleteRevision(String cloudId, String representationName, String version, String revisionName,
      String revisionProviderId, Date revisionTimestamp) throws RepresentationNotExistsException;


  /**
   * Inserts information to the all the tables which has dataset and revisions entries
   *
   * @param globalId cloud identifier
   * @param schema representation name
   * @param version version identifier
   * @param revision revision object containing necessary info (name, timestamp, tags)
   * @throws RepresentationNotExistsException
   */
  void updateAllRevisionDatasetsEntries(String globalId, String schema, String version, Revision revision)
      throws RepresentationNotExistsException;

  /**
   * @return
   */
  List<CompoundDataSetId> getAllDatasetsForRepresentationVersion(Representation representation)
      throws RepresentationNotExistsException;

  Collection<CompoundDataSetId> getDataSetAssignmentsByRepresentationVersion(String cloudId, String schemaId, String version)
      throws RepresentationNotExistsException;

  /**
   * Returns one (usually the first one from DB) for the given representation
   *
   * @param cloudId cloud identifier to be used
   * @param representationName representation name to be used
   * @return found data set
   * @throws RepresentationNotExistsException in case of non-existing representation version
   */
  Optional<CompoundDataSetId> getOneDatasetFor(String cloudId, String representationName) throws RepresentationNotExistsException;

  public void checkIfDatasetExists(String dataSetId, String providerId) throws DataSetNotExistsException;
}
