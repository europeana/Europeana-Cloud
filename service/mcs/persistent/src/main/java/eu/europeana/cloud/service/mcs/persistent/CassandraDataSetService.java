package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Implementation of data set service using Cassandra database.
 */
@Service
public class CassandraDataSetService implements DataSetService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataSetService.class);

    @Autowired
    private CassandraDataSetDAO dataSetDAO;

    @Autowired
    private CassandraRecordDAO recordDAO;

    @Autowired
    private UISClientHandler uis;

    /**
     * @inheritDoc
     */
    @Override
    public ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit)
            throws DataSetNotExistsException {
        checkIfDatasetExists(dataSetId, providerId);

        // get representation stubs from data set
        List<Properties> representationStubs = dataSetDAO.listDataSet(providerId, dataSetId, thresholdParam, limit);

        // if this is not last slice of result - add reference to next one by
        // encoding parameters in thresholdParam
        String nextResultToken = null;
        if (representationStubs.size() == limit + 1) {
            nextResultToken = representationStubs.get(limit).getProperty("nextSlice");
            representationStubs.remove(limit);
        }

        // replace representation stubs with real representations
        List<Representation> representations = new ArrayList<>(representationStubs.size());
        for (Properties stub : representationStubs) {
            if (stub.getProperty("versionId") == null) {
                representations.add(recordDAO.getLatestPersistentRepresentation(stub.getProperty("cloudId"),
                        stub.getProperty("schema")));
            } else {
                representations.add(recordDAO.getRepresentation(stub.getProperty("cloudId"), stub.getProperty("schema"),
                        stub.getProperty("versionId")));
            }
        }
        return new ResultSlice<Representation>(nextResultToken, representations);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void addAssignment(String providerId, String dataSetId,
                              String recordId, String schema, String version)
            throws DataSetNotExistsException, RepresentationNotExistsException {

        checkIfDatasetExists(dataSetId, providerId);
        Representation rep = getRepresentationIfExist(recordId, schema, version);

        if (!isAssignmentExists(providerId, dataSetId, recordId, schema, rep.getVersion())) {
            // now - when everything is validated - add assignment
            dataSetDAO.addAssignment(providerId, dataSetId, recordId, schema,
                    rep.getVersion());
            DataProvider dataProvider = uis.getProvider(providerId);
            dataSetDAO.addDataSetsRepresentationName(providerId, dataSetId, schema);

            Map<String, Revision> latestRevisions = new HashMap<>();

            for (Revision revision : rep.getRevisions()) {
                String revisionKey = revision.getRevisionName() + "_" + revision.getRevisionProviderId();
                Revision currentRevision = latestRevisions.get(revisionKey);
                if (currentRevision == null || revision.getCreationTimeStamp().getTime() > currentRevision.getCreationTimeStamp().getTime()) {
                    latestRevisions.put(revisionKey, revision);
                }
                dataSetDAO.addDataSetsRevision(providerId, dataSetId, revision,
                        schema, recordId);

                dataSetDAO.insertProviderDatasetRepresentationInfo(dataSetId, providerId, recordId, rep.getVersion(), schema,
                        RevisionUtils.getRevisionKey(revision), revision.getCreationTimeStamp(),
                        revision.isAcceptance(), revision.isPublished(), revision.isDeleted());
                dataSetDAO.addLatestRevisionForDatasetAssignment(dataSetDAO.getDataSet(providerId, dataSetId), rep, revision);
            }
            updateLatestProviderDatasetRevisions(providerId, dataSetId, recordId, schema, version, latestRevisions);
        }
    }

    private boolean isAssignmentExists(String providerId, String dataSetId, String recordId, String schema, String version) {
        Map<String, Set<String>> providerDataSets = dataSetDAO.getDataSets(recordId, schema, version);
        if (!providerDataSets.isEmpty()) {
            Set<String> dataSets = providerDataSets.get(providerId);
            if (dataSets.contains(dataSetId)) {
                return true;
            }
        }
        return false;
    }

    private void updateLatestProviderDatasetRevisions(String providerId, String dataSetId, String recordId, String schema, String version, Map<String, Revision> latestRevisions) {
        //
        Representation rep = new Representation();
        rep.setCloudId(recordId);
        rep.setRepresentationName(schema);
        rep.setVersion(version);
        DataSet ds = new DataSet();
        ds.setId(dataSetId);
        ds.setProviderId(providerId);
        //
        if (!latestRevisions.isEmpty()) {
            for (Revision latestRevision : latestRevisions.values()) {
                Date latestStoredRevisionTimestamp = dataSetDAO.getLatestRevisionTimeStamp(dataSetId, providerId, schema, latestRevision.getRevisionName(), latestRevision.getRevisionProviderId(), latestRevision.isDeleted(), recordId);
                if (latestStoredRevisionTimestamp == null || latestStoredRevisionTimestamp.getTime() < latestRevision.getCreationTimeStamp().getTime()) {
                    dataSetDAO.insertLatestProviderDatasetRepresentationInfo(dataSetId, providerId,
                            recordId, schema, latestRevision.getRevisionName(), latestRevision.getRevisionProviderId(), latestRevision.getCreationTimeStamp(), version,
                            latestRevision.isAcceptance(), latestRevision.isPublished(), latestRevision.isDeleted());
                    dataSetDAO.addLatestRevisionForDatasetAssignment(ds, rep, latestRevision);
                }
            }
        }

    }

    private Representation getRepresentationIfExist(String recordId, String schema, String version) throws RepresentationNotExistsException {
        Representation rep;
        if (version == null) {
            rep = recordDAO.getLatestPersistentRepresentation(recordId, schema);
            if (rep == null) {
                throw new RepresentationNotExistsException();
            }
        } else {
            rep = recordDAO.getRepresentation(recordId, schema, version);

            if (rep == null) {
                throw new RepresentationNotExistsException();
            }
        }
        return rep;
    }


    /**
     * @inheritDoc
     */
    @Override
    public void removeAssignment(String providerId, String dataSetId,
                                 String recordId, String schema, String versionId) throws DataSetNotExistsException {
        checkIfDatasetExists(dataSetId, providerId);

        dataSetDAO.removeAssignment(providerId, dataSetId, recordId, schema, versionId);
        if (!dataSetDAO.hasMoreRepresentations(providerId, dataSetId, schema)) {
            dataSetDAO.removeRepresentationNameForDataSet(schema, providerId, dataSetId);
        }

        Representation representation = recordDAO.getRepresentation(recordId, schema, versionId);

        Set<Revision> deletedRevisions = new HashSet<>();
        if (representation != null) {
            for (Revision revision : representation.getRevisions()) {
                String revisionName = revision.getRevisionName();
                String revisionProvider = revision.getRevisionProviderId();
                String revisionId = revisionName + "_" + revisionProvider;
                if (!deletedRevisions.contains(revisionId)) {
                    dataSetDAO.deleteLatestProviderDatasetRepresentationInfo(dataSetId, providerId,
                            recordId, schema, revisionName, revisionProvider, revision.isDeleted());
                    deletedRevisions.add(revision);
                }
                dataSetDAO.removeDataSetsRevision(providerId, dataSetId, revision, schema, recordId);
                dataSetDAO.deleteProviderDatasetRepresentationInfo(dataSetId, providerId, recordId, schema, revision.getCreationTimeStamp());
                DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
                dataSetDAO.removeLatestRevisionForDatasetAssignment(ds, representation, revision);
            }
        }

        //
        DataSet dataSet = new DataSet();
        dataSet.setProviderId(providerId);
        dataSet.setId(dataSetId);
        //
        findNewRepresentationVersionsWithLatestRevisions(dataSet, deletedRevisions, representation);
    }

    private void findNewRepresentationVersionsWithLatestRevisions(DataSet dataSet, Set<Revision> deletedRevisions, Representation representation) {
        if (!deletedRevisions.isEmpty()) {
            for (Revision deletedRevision : deletedRevisions) {
                findNewRepresentationVersionWithLatestRevision(representation, deletedRevision, dataSet);
            }
        }
    }

    private void findNewRepresentationVersionWithLatestRevision(Representation unassignedRepresentation, Revision revision, DataSet dataset) {

        List<Representation> representations = recordDAO.getAllRepresentationVersionsForRevisionName(
                unassignedRepresentation.getCloudId(),
                unassignedRepresentation.getRepresentationName(),
                revision,
                null);

        for (Representation representation : representations) {
            if (!isRepresentationBeingRemoved(representation, unassignedRepresentation)) {
                if (isRepresentationInsideDataSet(representation, dataset)) {
                    insertRevisionAsLatestForRepresentation(representation, revision, dataset);
                    break;
                }
            }
        }
    }

    private boolean isRepresentationBeingRemoved(Representation foundRepresentation, Representation removedRepresentation) {
        if (foundRepresentation.getVersion().equals(removedRepresentation.getVersion())) {
            return true;
        } else
            return false;
    }

    private boolean isRepresentationInsideDataSet(Representation rep, DataSet dataSet) {
        try {
            Collection<CompoundDataSetId> datasetIds = dataSetDAO.getDataSetAssignmentsByRepresentationVersion(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
            CompoundDataSetId compoundDataSetId = new CompoundDataSetId(dataSet.getProviderId(), dataSet.getId());
            if (datasetIds.contains(compoundDataSetId)) {
                return true;
            } else {
                return false;
            }
        } catch (RepresentationNotExistsException e) {
            LOGGER.error(e.getMessage());
        }
        return false;
    }

    private void insertRevisionAsLatestForRepresentation(Representation rep, Revision rev, DataSet dataset) {
        dataSetDAO.insertLatestProviderDatasetRepresentationInfo(dataset.getId(), dataset.getProviderId(),
                rep.getCloudId(), rep.getRepresentationName(), rev.getRevisionName(), rev.getRevisionProviderId(), rev.getCreationTimeStamp(), rep.getVersion(),
                rev.isAcceptance(), rev.isPublished(), rev.isDeleted());
        dataSetDAO.addLatestRevisionForDatasetAssignment(dataset, rep, rev);
    }

    /**
     * >>>>>>> develop
     *
     * @inheritDoc
     */
    @Override
    public DataSet createDataSet(String providerId, String dataSetId,
                                 String description) throws ProviderNotExistsException,
            DataSetAlreadyExistsException {
        Date now = new Date();
        if (uis.getProvider(providerId) == null) {
            throw new ProviderNotExistsException();
        }

        // check if dataset exists
        DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
        if (ds != null) {
            throw new DataSetAlreadyExistsException();
        }

        return dataSetDAO
                .createDataSet(providerId, dataSetId, description, now);
    }

    /**
     * @inheritDoc
     */
    @Override
    public DataSet updateDataSet(String providerId, String dataSetId,
                                 String description) throws DataSetNotExistsException {
        Date now = new Date();

        // check if dataset exists
        DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
        if (ds == null) {
            throw new DataSetNotExistsException("Provider " + providerId
                    + " does not have data set with id " + dataSetId);
        }
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
        return new ResultSlice<DataSet>(nextDataSet, dataSets);
    }

    @Override
    public Map<String, Set<String>> getDataSets(String cloudId, String representationName, String version) {
        return dataSetDAO.getDataSets(cloudId, representationName, version);
    }

    @Override
    public ResultSlice<CloudTagsResponse> getDataSetsRevisions(String providerId, String dataSetId, String revisionProviderId, String revisionName, Date revisionTimestamp, String representationName, String startFrom, int limit)
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
        List<Properties> list = dataSetDAO.getDataSetsRevisions(providerId, dataSetId, revisionProviderId, revisionName, revisionTimestamp, representationName, startFrom, limit);

        String nextToken = null;

        // when the list size is one element bigger than requested it means there is going to be next slice
        if (list.size() == limit + 1) {
            // set token to the last from list
            nextToken = list.get(limit).getProperty("nextSlice");
            // remove last element of the list
            list.remove(limit);
        }
        return new ResultSlice<>(nextToken, prepareCloudTagsResponseList(list));
    }

    private List<CloudTagsResponse> prepareCloudTagsResponseList(List<Properties> list) {
        List<CloudTagsResponse> result = new ArrayList<>(list.size());

        for (Properties properties : list) {
            result.add(new CloudTagsResponse(properties.getProperty("cloudId"),
                    Boolean.valueOf(properties.getProperty("published")), Boolean.valueOf(properties.getProperty("deleted")), Boolean.valueOf(properties.getProperty("acceptance"))));
        }

        return result;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void addDataSetsRevisions(String providerId, String dataSetId, Revision revision,
                                     String representationName, String cloudId)
            throws ProviderNotExistsException {
        if (uis.getProvider(providerId) == null) {
            throw new ProviderNotExistsException();
        }
        dataSetDAO.addDataSetsRevision(providerId, dataSetId, revision, representationName, cloudId);
    }


    @Override
    public ResultSlice<CloudVersionRevisionResponse> getDataSetCloudIdsByRepresentationPublished(String
                                                                                                         dataSetId, String providerId, String representationName, Date dateFrom, String startFrom,
                                                                                                 int numberOfElementsPerPage)
            throws ProviderNotExistsException, DataSetNotExistsException {
        // check whether provider and data set exist
        validateRequest(dataSetId, providerId);

        // run the query requesting one more element than items per page to determine the starting cloud id for the next slice
        List<Properties> list = dataSetDAO.getDataSetCloudIdsByRepresentationPublished(providerId, dataSetId, representationName, dateFrom, startFrom, numberOfElementsPerPage);

        String nextToken = null;

        // when the list size is one element bigger than requested it means there is going to be next slice
        if (list.size() == numberOfElementsPerPage + 1) {
            // set token to the last from list
            nextToken = list.get(numberOfElementsPerPage).getProperty("nextSlice");
            // remove last element of the list
            list.remove(numberOfElementsPerPage);
        }
        return new ResultSlice<>(nextToken, prepareResponseList(list));
    }


    @Override
    public void updateAllRevisionDatasetsEntries(String globalId, String schema, String
            version, Revision revision)
            throws RepresentationNotExistsException {
        Representation rep = recordDAO.getRepresentation(globalId, schema, version);
        if (rep == null) {
            throw new RepresentationNotExistsException(schema);
        }

        // collect data sets the version is assigned to
        Collection<CompoundDataSetId> dataSets = dataSetDAO.getDataSetAssignments(globalId, schema, version);
        // now we have to insert rows for each data set
        for (CompoundDataSetId dsID : dataSets) {
            String datasetName = dsID.getDataSetId();
            String datasetProvider = dsID.getDataSetProviderId();
            String revisionId = RevisionUtils.getRevisionKey(revision);
            dataSetDAO.insertProviderDatasetRepresentationInfo(datasetName, datasetProvider,
                    globalId, version, schema, revisionId, revision.getCreationTimeStamp(),
                    revision.isAcceptance(), revision.isPublished(), revision.isDeleted());
            dataSetDAO.insertLatestProviderDatasetRepresentationInfo(datasetName, datasetProvider,
                    globalId, schema, revision.getRevisionName(), revision.getRevisionProviderId(), revision.getCreationTimeStamp(), version,
                    revision.isAcceptance(), revision.isPublished(), revision.isDeleted());
            dataSetDAO.addDataSetsRevision(datasetProvider, datasetName, revision, schema, globalId);
            dataSetDAO.addLatestRevisionForDatasetAssignment(dataSetDAO.getDataSet(datasetProvider, datasetName), rep, revision);

        }
    }

    @Override
    public void deleteDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException {
        checkIfDatasetExists(dataSetId, providerId);
        String nextToken = null;
        int maxSize = 10000;

        List<Properties> representations = dataSetDAO.listDataSet(providerId, dataSetId, null, maxSize);
        for (Properties representation : representations) {
            removeAssignment(providerId, dataSetId, representation.getProperty("cloudId"), representation.getProperty("schema"), representation.getProperty("versionId"));
        }

        while (representations.size() == maxSize + 1) {
            nextToken = representations.get(maxSize).getProperty("nextSlice");
            representations = dataSetDAO.listDataSet(providerId, dataSetId, nextToken, maxSize);
            for (Properties representation : representations) {
                removeAssignment(providerId, dataSetId, representation.getProperty("cloudId"), representation.getProperty("schema"), representation.getProperty("versionId"));
            }
        }
        dataSetDAO.deleteDataSet(providerId, dataSetId);
    }

    @Override
    public Set<String> getAllDataSetRepresentationsNames(String providerId, String dataSetId) throws
            ProviderNotExistsException, DataSetNotExistsException {
        if (isProviderExists(providerId) && isDataSetExists(providerId, dataSetId)) {
            return dataSetDAO.getAllRepresentationsNamesForDataSet(providerId, dataSetId);
        }
        return Collections.emptySet();
    }


    private List<CloudVersionRevisionResponse> prepareResponseList(List<Properties> list) {
        List<CloudVersionRevisionResponse> result = new ArrayList<>(list.size());

        for (Properties properties : list) {
            result.add(new CloudVersionRevisionResponse(properties.getProperty("cloudId"),
                    properties.getProperty("versionId"),
                    properties.getProperty("revisionId"),
                    (Boolean) properties.get("published"),
                    (Boolean) properties.get("deleted"),
                    (Boolean) properties.get("acceptance")));
        }

        return result;
    }

    @Override
    public void updateProviderDatasetRepresentation(String globalId, String schema, String version, Revision revision)
            throws RepresentationNotExistsException {
        // check whether representation exists
        Representation rep = recordDAO.getRepresentation(globalId, schema, version);
        if (rep == null) {
            throw new RepresentationNotExistsException(schema);
        }

        // collect data sets the version is assigned to
        Collection<CompoundDataSetId> dataSets = dataSetDAO.getDataSetAssignments(globalId, schema, version);

        // now we have to insert rows for each data set
        for (CompoundDataSetId dsID : dataSets) {
            dataSetDAO.insertProviderDatasetRepresentationInfo(dsID.getDataSetId(), dsID.getDataSetProviderId(),
                    globalId, version, schema, RevisionUtils.getRevisionKey(revision), revision.getCreationTimeStamp(),
                    revision.isAcceptance(), revision.isPublished(), revision.isDeleted());
        }
    }

    @Override
    public String getLatestVersionForGivenRevision(String dataSetId, String providerId, String cloudId, String
            representationName, String revisionName, String revisionProviderId) throws DataSetNotExistsException {
        if (isDataSetExists(providerId, dataSetId)) {
            DataSet dataset = new DataSet();
            dataset.setProviderId(providerId);
            dataset.setId(dataSetId);
            //
            Representation rep = new Representation();
            rep.setCloudId(cloudId);
            rep.setRepresentationName(representationName);
            //
            Revision revision = new Revision();
            revision.setRevisionName(revisionName);
            revision.setRevisionProviderId(revisionProviderId);

            DataSetRepresentationForLatestRevision result = dataSetDAO.getRepresentationForLatestRevisionFromDataset(dataset, rep, revision);
            if (result != null) {
                return result.getRepresentation().getVersion();
            }
        }
        return null;
    }

    @Override
    public void addLatestRevisionForGivenVersionInDataset(DataSet dataSet, Representation representation, Revision
            revision) {
        dataSetDAO.addLatestRevisionForDatasetAssignment(dataSet, representation, revision);
    }

    private boolean isProviderExists(String providerId) throws ProviderNotExistsException {
        if (!uis.existsProvider(providerId)) {
            throw new ProviderNotExistsException();
        }
        return true;
    }

    private boolean isDataSetExists(String providerId, String dataSetId) throws DataSetNotExistsException {
        checkIfDatasetExists(dataSetId, providerId);
        return true;
    }

    private String encodeParams(String... parts) {
        byte[] paramsJoined = Joiner.on('\n').join(parts)
                .getBytes(Charset.forName("UTF-8"));
        return BaseEncoding.base32().encode(paramsJoined);

    }

    private List<String> decodeParams(String encodedParams) {
        byte[] paramsDecoded = BaseEncoding.base32().decode(encodedParams);
        String paramsDecodedString = new String(paramsDecoded,
                Charset.forName("UTF-8"));
        return Splitter.on('\n').splitToList(paramsDecodedString);
    }


    /**
     * get a list of the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision.
     * This list will contain one row per revision per cloudId;
     *
     * @param dataSetId               data set identifier
     * @param providerId              provider identifier
     * @param revisionName            revision name
     * @param revisionProvider        revision provider
     * @param representationName      representation name
     * @param startFrom               cloudId to start from
     * @param isDeleted               revision marked-deleted
     * @param numberOfElementsPerPage number of elements in a slice
     * @return slice of the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision.
     * This list will contain one row per revision per cloudId ;
     * @throws ProviderNotExistsException
     * @throws DataSetNotExistsException
     */

    @Override
    public ResultSlice<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevision(String dataSetId, String providerId, String revisionName, String revisionProvider, String representationName, String startFrom, Boolean isDeleted, int numberOfElementsPerPage)
            throws ProviderNotExistsException, DataSetNotExistsException {

        validateRequest(dataSetId, providerId);
        List<CloudIdAndTimestampResponse> list = dataSetDAO.getLatestDataSetCloudIdByRepresentationAndRevision(providerId, dataSetId, revisionName, revisionProvider, representationName, startFrom, isDeleted, numberOfElementsPerPage);
        String nextToken = null;
        if (list.size() == numberOfElementsPerPage + 1) {
            nextToken = list.get(numberOfElementsPerPage).getCloudId();
            list.remove(numberOfElementsPerPage);
        }
        return new ResultSlice<>(nextToken, list);
    }


    @Override
    public void deleteRevision(String cloudId, String representationName, String version, String revisionName, String revisionProviderId, Date revisionTimestamp)
            throws RepresentationNotExistsException {

        checkIfRepresentationExists(representationName, version, cloudId);
        Revision revision = new Revision(revisionName, revisionProviderId);
        revision.setCreationTimeStamp(revisionTimestamp);

        Collection<CompoundDataSetId> compoundDataSetIds = dataSetDAO.getDataSetAssignmentsByRepresentationVersion(cloudId, representationName, version);
        for (CompoundDataSetId compoundDataSetId : compoundDataSetIds) {

            //data_set_assignments_by_revision_id_v1
            dataSetDAO.removeDataSetsRevision(compoundDataSetId.getDataSetProviderId(), compoundDataSetId.getDataSetId(), revision, representationName, cloudId);

            //provider_dataset_representation
            dataSetDAO.deleteProviderDatasetRepresentationInfo(compoundDataSetId.getDataSetId(), compoundDataSetId.getDataSetProviderId(), cloudId, representationName, revisionTimestamp);

        }

        //representation revisions
        recordDAO.deleteRepresentationRevision(cloudId, representationName, version, revisionProviderId, revisionName, revisionTimestamp);

        //representation version
        recordDAO.removeRevisionFromRepresentationVersion(cloudId, representationName, version,revision);


    }

    private void checkIfRepresentationExists(String representationName, String version, String cloudId) throws RepresentationNotExistsException {
        Representation rep = recordDAO.getRepresentation(cloudId, representationName, version);
        if (rep == null) {
            throw new RepresentationNotExistsException();
        }
    }

    private void checkIfDatasetExists(String dataSetId, String providerId) throws DataSetNotExistsException {
        DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
        if (ds == null) {
            throw new DataSetNotExistsException();
        }
    }

    private void validateRequest(String dataSetId, String providerId) throws ProviderNotExistsException, DataSetNotExistsException {
        if (!uis.existsProvider(providerId)) {
            throw new ProviderNotExistsException("Provider doesn't exist " + providerId);
        }

        if (dataSetDAO.getDataSet(providerId, dataSetId) == null) {
            throw new DataSetNotExistsException("Data set " + dataSetId + " doesn't exist for provider " + providerId);
        }
    }


}
