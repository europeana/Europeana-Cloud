package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.DatasetAssignment;

import java.util.*;

import static eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO.*;
import static eu.europeana.cloud.service.mcs.persistent.cassandra.PersistenceUtils.createProviderDataSetId;
import static java.util.function.Predicate.not;

/**
 * Implementation of data set service using Cassandra database.
 */
public class CassandraDataSetService implements DataSetService {

  private final CassandraDataSetDAO dataSetDAO;
  private final CassandraRecordDAO recordDAO;
  private final UISClientHandler uis;
  private final BucketsHandler bucketsHandler;

  public CassandraDataSetService(CassandraDataSetDAO dataSetDAO, CassandraRecordDAO recordDAO, UISClientHandler uis,
      BucketsHandler bucketsHandler) {
    this.dataSetDAO = dataSetDAO;
    this.recordDAO = recordDAO;
    this.uis = uis;
    this.bucketsHandler = bucketsHandler;
  }

  /**
   * @inheritDoc
   */
  @Override
  public ResultSlice<Representation> listDataSet(String providerId, String dataSetId,
                                                 String thresholdParam, boolean existingOnly, int limit) throws DataSetNotExistsException {
    checkIfDatasetExists(dataSetId, providerId);
    // get representation stubs from data set
    ResultSlice<DatasetAssignment> assignments = listDataSetAssignments(providerId, dataSetId, thresholdParam, limit, existingOnly);
    // replace representation stubs with real representations
    if (existingOnly){
      return new ResultSlice<>(assignments.getNextSlice(), getExistingRepresentations(assignments.getResults()));
    } else {
      return new ResultSlice<>(assignments.getNextSlice(), getRepresentations(assignments.getResults()));
    }
  }

  @Override
  public void updateAllRevisionDatasetsEntries(String cloudId, String schema, String version, Revision revision)
      throws RepresentationNotExistsException {

    Representation rep = recordDAO.getRepresentation(cloudId, schema, version);
    if (rep == null) {
      throw new RepresentationNotExistsException(schema);
    }
    addDataSetRevision(rep.getDataProvider(), rep.getDatasetId(), revision, schema, cloudId, version);

  }

  /**
   * Adds representation to a data set. Might add representation in the latest persistent or specified version. Does not do any
   * kind of parameter validation - specified data set and representation version must exist before invoking this method.
   *
   * @param providerId data set owner's (provider's) id
   * @param dataSetId data set id
   * @param recordId record id
   * @param schema representation schema
   * @param version representation version (might be null if newest version is to be assigned)
   */
  public void addAssignmentToMainTables(String providerId, String dataSetId, String recordId, String schema, String version, boolean markDeleted)
      throws NoHostAvailableException, QueryExecutionException {

    Date now = Calendar.getInstance().getTime();
    String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
    UUID versionId = null;
    if (version != null) {
      versionId = UUID.fromString(version);
    }

    Bucket bucket = bucketsHandler.getCurrentBucket(DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, providerDataSetId);
    // when there is no bucket or bucket rows count is max we should add another bucket
    if (bucket == null || bucket.getRowsCount() >= MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT) {
      bucket = new Bucket(providerDataSetId, createBucket(), 0);
    }
    bucketsHandler.increaseBucketCount(DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, bucket);

    dataSetDAO.addAssignment(providerId, dataSetId, bucket.getBucketId(), recordId, schema, now, versionId, markDeleted);
  }

  public void addDataSetRevision(String providerId, String datasetId, Revision revision, String representationName,
                                 String cloudId, String versionId) {
    //
    String providerDataSetId = createProviderDataSetId(providerId, datasetId);
    Bucket bucket = bucketsHandler.getCurrentBucket(DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS, providerDataSetId);
    // when there is no bucket or bucket rows count is max we should add another bucket
    if (bucket == null || bucket.getRowsCount() >= MAX_DATASET_ASSIGNMENTS_BY_REVISION_ID_BUCKET_COUNT) {
      bucket = new Bucket(providerDataSetId, createBucket(), 0);
    }
    bucketsHandler.increaseBucketCount(DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS, bucket);
    //
    dataSetDAO.addDataSetsRevision(providerId, datasetId, bucket.getBucketId(), revision, representationName, cloudId, versionId);
  }


  /**
   * @inheritDoc
   */
  @Override
  public void removeAssignment(String providerId, String dataSetId,
      String recordId, String schema, String versionId) throws DataSetNotExistsException {
    checkIfDatasetExists(dataSetId, providerId);

    removeAssignmentFromMainTables(providerId, dataSetId, recordId, schema, versionId);

    Representation representation = recordDAO.getRepresentation(recordId, schema, versionId);

    if (representation != null) {
      for (Revision revision : representation.getRevisions()) {
        removeDataSetsRevision(providerId, dataSetId, revision, schema, recordId, versionId);
      }
    }

  }

  @Override
  public DataSet createDataSet(String providerId, String dataSetId,
      String description) throws ProviderNotExistsException,
      DataSetAlreadyExistsException {
    if (uis.getProvider(providerId) == null) {
      throw new ProviderNotExistsException();
    }

    // check if dataset exists
    DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
    if (ds != null) {
      throw new DataSetAlreadyExistsException("Data set with provided name already exists");
    }

    Date now = new Date();
    return dataSetDAO
        .createDataSet(providerId, dataSetId, description, now);
  }

  /**
   * @inheritDoc
   */
  @Override
  public DataSet updateDataSet(String providerId, String dataSetId,
      String description) throws DataSetNotExistsException {

    // check if dataset exists
    DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
    if (ds == null) {
      throw new DataSetNotExistsException("Provider " + providerId
          + " does not have data set with id " + dataSetId);
    }
    Date now = new Date();
    return dataSetDAO
        .createDataSet(providerId, dataSetId, description, now);
  }

  /**
   * @inheritDoc
   */
  @Override
  public ResultSlice<DataSet> getDataSets(String providerId,
      String thresholdDatasetId, int limit) {

    List<DataSet> dataSets = dataSetDAO.getDataSets(providerId,
        thresholdDatasetId, limit + 1);
    String nextDataSet = null;
    if (dataSets.size() == limit + 1) {
      DataSet nextResult = dataSets.get(limit);
      nextDataSet = nextResult.getId();
      dataSets.remove(limit);
    }
    return new ResultSlice<>(nextDataSet, dataSets);
  }

  @Override
  public ResultSlice<CloudTagsResponse> getDataSetsRevisions(String providerId, String dataSetId, String revisionProviderId,
      String revisionName, Date revisionTimestamp, String representationName,
      String startFrom, int limit)
      throws ProviderNotExistsException, DataSetNotExistsException {
    // check whether provider exists
    if (!uis.existsProvider(providerId)) {
      throw new ProviderNotExistsException("Provider doesn't exist " + providerId);
    }

    // check whether data set exists
    if (dataSetDAO.getDataSet(providerId, dataSetId) == null) {
      throw new DataSetNotExistsException("Data set " + dataSetId + " doesn't exist for provider " + providerId);
    }

    // run the query requesting one more element than items per page to determine the starting cloud id for the next slice
    return getDataSetsRevisionsPage(providerId, dataSetId, revisionProviderId, revisionName, revisionTimestamp,
        representationName, startFrom, limit);
  }

  @Override
  public List<CloudTagsResponse> getDataSetsExistingRevisions(
      String providerId, String dataSetId, String revisionProviderId, String revisionName, Date revisionTimestamp,
      String representationName, int limit) throws ProviderNotExistsException, DataSetNotExistsException {

    List<CloudTagsResponse> resultList = new ArrayList<>();
    ResultSlice<CloudTagsResponse> subResults;
    String startFrom = null;

    do {
      subResults = getDataSetsRevisions(providerId, dataSetId, revisionProviderId, revisionName, revisionTimestamp,
          representationName, startFrom, 5000);

      subResults.getResults().stream().filter(not(CloudTagsResponse::isDeleted)).limit((long) limit - resultList.size())
                .forEach(resultList::add);
      startFrom = subResults.getNextSlice();
    } while (startFrom != null && resultList.size() < limit);

    return resultList;
  }
  @Override
  public Optional<CompoundDataSetId> getOneDatasetFor(String cloudId, String representationName, UUID version) {
    return recordDAO.getRepresentationDatasetId(cloudId, representationName, version);
  }

  @Override
  public void deleteDataSet(String providerId, String dataSetId)
      throws DataSetDeletionException, DataSetNotExistsException {

    checkIfDatasetExists(dataSetId, providerId);
    if (datasetIsEmpty(providerId, dataSetId)) {
      dataSetDAO.deleteDataSet(providerId, dataSetId);
    } else {
      throw new DataSetDeletionException("Can't do it. Dataset is not empty");
    }
  }


  @Override
  public void deleteRevision(String cloudId, String representationName, String version, String revisionName,
      String revisionProviderId, Date revisionTimestamp)
      throws RepresentationNotExistsException {

    Representation rep = recordDAO.getRepresentation(cloudId, representationName, version);
    if (rep == null) {
      throw new RepresentationNotExistsException();
    }

    Revision revision = new Revision(revisionName, revisionProviderId);
    revision.setCreationTimeStamp(revisionTimestamp);

    removeDataSetsRevision(rep.getDataProvider(), rep.getDatasetId(), revision, representationName, cloudId, version);

    //representation revisions
    recordDAO.deleteRepresentationRevision(cloudId, representationName, version, revisionProviderId, revisionName,
            revisionTimestamp);

    //representation version
    recordDAO.removeRevisionFromRepresentationVersion(cloudId, representationName, version, revision);


  }

  public void removeDataSetsRevision(String providerId, String datasetId, Revision revision, String representationName,
      String cloudId, String versionId) {

    List<Bucket> availableBuckets = bucketsHandler.getAllBuckets(
        DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS, createProviderDataSetId(providerId, datasetId));

    for (Bucket bucket : availableBuckets) {
      if (dataSetDAO.removeDataSetRevision(providerId, datasetId, bucket.getBucketId(), revision, representationName, cloudId, versionId)) {
        bucketsHandler.decreaseBucketCount(DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS, bucket);
        return;
      }
    }
  }
  public void checkIfDatasetExists(String dataSetId, String providerId) throws DataSetNotExistsException {
    DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
    if (ds == null) {
      throw new DataSetNotExistsException();
    }
  }

  private List<Representation> getRepresentations(List<DatasetAssignment> assignments) {
    return assignments.stream()
                      .map(stub -> recordDAO.getRepresentation(stub.getCloudId(), stub.getSchema(), stub.getVersion()))
            .toList();
  }

  private List<Representation> getExistingRepresentations(List<DatasetAssignment> assignments) {
    return assignments.stream()
            .map(stub -> recordDAO.getRepresentation(stub.getCloudId(), stub.getSchema(), stub.getVersion()))
            .filter(representation -> !representation.getFiles().isEmpty())
                      .toList();
  }

  /**
   * Returns stubs of representations assigned to a data set. Stubs contain cloud id and schema of the representation, may also
   * contain version (if a certain version is in a data set).
   *
   * @param providerId data set owner's (provider's) id
   * @param dataSetId data set id
   * @param nextToken next token containing information about paging state and bucket id
   * @param limit maximum size of returned list
   * @param existingOnly whether should return only assignments not marked as deleted
   * @return ResultSlice
   */
  private ResultSlice<DatasetAssignment> listDataSetAssignments(String providerId, String dataSetId, String nextToken, int limit, boolean existingOnly)
      throws NoHostAvailableException, QueryExecutionException {
    String id = createProviderDataSetId(providerId, dataSetId);
    return loadPage(id, nextToken, limit, DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS,
        (bucket, pagingState, localLimit) ->
            dataSetDAO.getDataSetAssignments(id, bucket.getBucketId(), pagingState, localLimit, existingOnly));
  }

  /**
   * Removes representation from data set (regardless representation version).
   *
   * @param providerId data set owner's (provider's) id
   * @param dataSetId data set id
   * @param recordId record's id
   * @param schema representation's schema
   */
  private void removeAssignmentFromMainTables(String providerId, String dataSetId, String recordId, String schema,
      String versionId)
      throws NoHostAvailableException, QueryExecutionException {

    String providerDataSetId = createProviderDataSetId(providerId, dataSetId);

    Bucket bucket = bucketsHandler.getFirstBucket(DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, providerDataSetId);

    while (bucket != null) {
      if (dataSetDAO.removeDatasetAssignment(recordId, schema, versionId, providerDataSetId, bucket)) {
        // remove bucket count
        bucketsHandler.decreaseBucketCount(DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, bucket);
        return;
      }
      bucket = bucketsHandler.getNextBucket(DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, providerDataSetId, bucket);
    }
  }

  private <E> ResultSlice<E> loadPage(String dataId, String nextToken, int limit,
      String bucketsTableName, OneBucketLoader<E> oneBucketLoader) {

    Bucket bucket;
    PagingState state;

    if (nextToken == null) {
      // there is no next token so do not set paging state, take the first bucket for provider's dataset
      bucket = bucketsHandler.getFirstBucket(bucketsTableName, dataId);
      state = null;
    } else {
      // next token is set, parse it to retrieve paging state and bucket id
      // (token is concatenation of paging state and bucket id using '_' character
      String[] parts = nextToken.split("_");
      if (parts.length != 2) {
        throw new IllegalArgumentException("nextToken format is wrong. nextToken = " + nextToken);
      }

      // first element is the paging state
      state = getPagingState(parts[0]);
      // second element is bucket id
      bucket = getAssignmentBucketId(bucketsTableName, parts[1], state, dataId);
    }

    List<E> result = new ArrayList<>(limit);
    // if the bucket is null it means we reached the end of data
    if (bucket == null) {
      return new ResultSlice<>(null, result);
    }

    ResultSlice<E> oneBucketResult = oneBucketLoader.loadData(bucket, state, limit);
    result.addAll(oneBucketResult.getResults());

    String resultNextSlice = null;
    if (result.size() == limit) {
      if (oneBucketResult.getNextSlice() != null) {
        // we reached the page limit, prepare the next slice string to be used for the next page
        resultNextSlice = oneBucketResult.getNextSlice() + "_" + bucket.getBucketId();
      } else {
        // we reached the end of bucket and limit - in this case if there are more buckets we should set proper nextSlice
        if (bucketsHandler.getNextBucket(bucketsTableName, dataId, bucket) != null) {
          resultNextSlice = "_" + bucket.getBucketId();
        }
      }
    } else {
      // we reached the end of bucket but number of results is less than the page size - in this case
      // if there are more buckets we should retrieve number of results that will feed the page
      if (bucketsHandler.getNextBucket(bucketsTableName, dataId, bucket) != null) {
        String nextSlice = "_" + bucket.getBucketId();
        ResultSlice<E> resultsFromFurtherBuckets =
            loadPage(dataId, nextSlice, limit - result.size(), bucketsTableName, oneBucketLoader);
        result.addAll(resultsFromFurtherBuckets.getResults());
        resultNextSlice = resultsFromFurtherBuckets.getNextSlice();
      }
    }
    return new ResultSlice<>(resultNextSlice, result);
  }

  /**
   * Get paging state from part of token. When the token is null or empty paging state is null. Otherwise, we can create paging
   * state from that string.
   *
   * @param tokenPart part of token containing string representation of paging state from previous query
   * @return null when token part is empty or null paging state otherwise
   */
  private PagingState getPagingState(String tokenPart) {
    if (tokenPart != null && !tokenPart.isEmpty()) {
      return PagingState.fromString(tokenPart);
    }
    return null;
  }

  /**
   * Get bucket id from part of token considering paging state which was retrieved from the same token. This is used for data
   * assignment table where provider id and dataset id are concatenated to one string
   *
   * @param bucketsTableName table name used for buckets
   * @param tokenPart part of token containing bucket id
   * @param state paging state from the same token as the bucket id
   * @param providerDataSetId provider id and dataset id to retrieve next bucket id
   * @return bucket id to be used for the query
   */
  private Bucket getAssignmentBucketId(String bucketsTableName, String tokenPart, PagingState state, String providerDataSetId) {
    if (tokenPart != null && !tokenPart.isEmpty()) {
      // when the state passed in the next token is not null we have to use the same bucket id as the paging state
      // is associated with the query having certain parameter values
      if (state != null) {
        return new Bucket(providerDataSetId, tokenPart, 0);
      }
      // the state part is empty which means we reached the end of the bucket passed in the next token,
      // therefore we need to get the next bucket
      return bucketsHandler.getNextBucket(bucketsTableName, providerDataSetId, new Bucket(providerDataSetId, tokenPart, 0));
    }
    return null;
  }

  private boolean datasetIsEmpty(String providerId, String dataSetId) {
    return listDataSetAssignments(providerId, dataSetId, null, 1, false).getResults().isEmpty();
  }

  private String createBucket() {
    return new com.eaio.uuid.UUID().toString();
  }

  public interface OneBucketLoader<E> {

    ResultSlice<E> loadData(Bucket bucket, PagingState pagingState, int localLimit);
  }

  ResultSlice<CloudTagsResponse> getDataSetsRevisionsPage(String providerId, String dataSetId, String revisionProviderId,
      String revisionName, Date revisionTimestamp, String representationName,
      String nextToken, int limit) {
    String id = createProviderDataSetId(providerId, dataSetId);
    return loadPage(id, nextToken, limit, DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS,
        (bucket, pagingState, localLimit) ->
            dataSetDAO.getDataSetsRevisions(providerId, dataSetId, bucket.getBucketId(), revisionProviderId,
                revisionName, revisionTimestamp, representationName, pagingState, localLimit));
  }
}
