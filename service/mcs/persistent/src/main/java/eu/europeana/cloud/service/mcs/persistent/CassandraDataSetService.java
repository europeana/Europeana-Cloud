package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.PersistenceUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

import static java.util.function.Predicate.not;

/**
 * Implementation of data set service using Cassandra database.
 */
@Service
public class CassandraDataSetService implements DataSetService {

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
    public ResultSlice<Representation> listDataSet(String providerId, String dataSetId,
                                                   String thresholdParam, int limit) throws DataSetNotExistsException {

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
        return new ResultSlice<>(nextResultToken, representations);
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
            dataSetDAO.addDataSetsRepresentationName(providerId, dataSetId, schema);

            for (Revision revision : rep.getRevisions()) {
                dataSetDAO.addDataSetsRevision(providerId, dataSetId, revision,
                        schema, recordId);
            }
        }
    }

    private boolean isAssignmentExists(String providerId, String dataSetId, String recordId, String schema, String version) {
        String seekedIdString = PersistenceUtils.createProviderDataSetId(providerId, dataSetId);
        CompoundDataSetId seekedId = PersistenceUtils.createCompoundDataSetId(seekedIdString);
        return dataSetDAO.getDataSetAssignments(recordId, schema, version).contains(seekedId);
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

        if (representation != null) {
            for (Revision revision : representation.getRevisions()) {
                dataSetDAO.removeDataSetsRevision(providerId, dataSetId, revision, schema, recordId);
            }
        }

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
            throw new DataSetAlreadyExistsException("Data set with provided name already exists");
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
        return new ResultSlice<>(nextDataSet, dataSets);
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

            subResults.getResults().stream().filter(not(CloudTagsResponse::isDeleted)).limit(limit - resultList.size())
                    .forEach(resultList::add);
            startFrom = subResults.getNextSlice();
        } while (startFrom != null && resultList.size() < limit);

        return resultList;
    }

    private List<CloudTagsResponse> prepareCloudTagsResponseList(List<Properties> list) {
        List<CloudTagsResponse> result = new ArrayList<>(list.size());

        for (Properties properties : list) {
            result.add(new CloudTagsResponse(properties.getProperty("cloudId"),
                    Boolean.valueOf(properties.getProperty("published")),
                    Boolean.valueOf(properties.getProperty("deleted")),
                    Boolean.valueOf(properties.getProperty("acceptance")) )
            );
        }

        return result;
    }

    @Override
    public void updateAllRevisionDatasetsEntries(String globalId, String schema, String version, Revision revision)
            throws RepresentationNotExistsException {

        Representation rep = recordDAO.getRepresentation(globalId, schema, version);
        if (rep == null) {
            throw new RepresentationNotExistsException(schema);
        }

        // collect data sets the version is assigned to
        Collection<CompoundDataSetId> dataSets = dataSetDAO.getDataSetAssignments(globalId, schema, version);

        // now we have to insert rows for each data set
        for (CompoundDataSetId dsID : dataSets) {
            dataSetDAO.addDataSetsRevision(dsID.getDataSetProviderId(), dsID.getDataSetId(), revision, schema, globalId);
        }
    }

    @Override
    public List<CompoundDataSetId> getAllDatasetsForRepresentationVersion(Representation representation) throws RepresentationNotExistsException {
        return new ArrayList<>(
                dataSetDAO.getDataSetAssignmentsByRepresentationVersion(
                        representation.getCloudId(),
                        representation.getRepresentationName(),
                        representation.getVersion())
        );
    }

    @Override
    public void deleteDataSet(String providerId, String dataSetId)
            throws DataSetDeletionException, DataSetNotExistsException {

        checkIfDatasetExists(dataSetId,providerId);
        if (datasetIsEmpty(providerId, dataSetId)) {
            dataSetDAO.deleteDataSet(providerId, dataSetId);
        } else {
            throw new DataSetDeletionException("Can't do it. Dataset is not empty");
        }
    }

    private boolean datasetIsEmpty(String providerId, String dataSetId) {
        return dataSetDAO.listDataSet(providerId, dataSetId, null, 1).isEmpty();
    }

    @Override
    public Set<String> getAllDataSetRepresentationsNames(String providerId, String dataSetId) throws
            ProviderNotExistsException, DataSetNotExistsException {
        if (isProviderExists(providerId) && isDataSetExists(providerId, dataSetId)) {
            return dataSetDAO.getAllRepresentationsNamesForDataSet(providerId, dataSetId);
        }
        return Collections.emptySet();
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
}
