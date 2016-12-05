package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import eu.europeana.cloud.common.model.*;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.*;

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
    private SolrRepresentationIndexer representationIndexer;

    @Autowired
    private UISClientHandler uis;

    /**
     * @inheritDoc
     */
    @Override
    public ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit)
            throws DataSetNotExistsException {
        DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);

        if (ds == null) {
            throw new DataSetNotExistsException();
        }

        // now - decode parameters encoded in thresholdParam
        String thresholdCloudId = null;
        String thresholdSchemaId = null;
        if (thresholdParam != null) {
            List<String> thresholdCloudIdAndSchema = decodeParams(thresholdParam);
            if (thresholdCloudIdAndSchema.size() != 2) {
                throw new IllegalArgumentException("Wrong threshold param!");
            }

            thresholdCloudId = thresholdCloudIdAndSchema.get(0);
            thresholdSchemaId = thresholdCloudIdAndSchema.get(1);
        }

        // get representation stubs from data set
        List<Representation> representationStubs = dataSetDAO.listDataSet(providerId, dataSetId, thresholdCloudId,
            thresholdSchemaId, limit + 1);

        // if this is not last slice of result - add reference to next one by
        // encoding parameters in thresholdParam
        String nextResultToken = null;
        if (representationStubs.size() == limit + 1) {
            Representation nextResult = representationStubs.get(limit);
            nextResultToken = encodeParams(nextResult.getCloudId(), nextResult.getRepresentationName());
            representationStubs.remove(limit);
        }

        // replace representation stubs with real representations
        List<Representation> representations = new ArrayList<>(representationStubs.size());
        for (Representation stub : representationStubs) {
            if (stub.getVersion() == null) {
                representations.add(recordDAO.getLatestPersistentRepresentation(stub.getCloudId(),
                    stub.getRepresentationName()));
            } else {
                representations.add(recordDAO.getRepresentation(stub.getCloudId(), stub.getRepresentationName(),
                    stub.getVersion()));
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
		DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);

		if (ds == null) {
			throw new DataSetNotExistsException();
		}

		// check if representation exists
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

		// now - when everything is validated - add assignment
		dataSetDAO.addAssignment(providerId, dataSetId, recordId, schema,
				rep.getVersion());
		DataProvider dataProvider = uis.getProvider(providerId);
		dataSetDAO.addDataSetsRepresentationName(providerId, dataSetId, schema);

		addDataSetAssignmentForAllRevisions(providerId, dataSetId, schema, rep);

		representationIndexer.addAssignment(rep.getVersion(),
				new CompoundDataSetId(providerId, dataSetId),
				dataProvider.getPartitionKey());
		for (Revision revision : rep.getRevisions())
			dataSetDAO.insertProviderDatasetRepresentationInfo(dataSetId, providerId, recordId, rep.getVersion(), schema,
					RevisionUtils.getRevisionKey(revision), revision.getUpdateTimeStamp(),
					revision.isAcceptance(), revision.isPublished(), revision.isDeleted());
	}

	private void addDataSetAssignmentForAllRevisions(String providerId, String dataSetId, String schema, Representation rep) {
		for (Revision revision : rep.getRevisions()) {
			if (revision != null) {
				dataSetDAO.addDataSetsRevision( providerId, dataSetId, RevisionUtils.getRevisionKey(revision),
						schema, rep.getCloudId());
			}
		}
	}

	/**
     * @inheritDoc
     */
	@Override
	public void removeAssignment(String providerId, String dataSetId,
								 String recordId, String schema, String versionId) throws DataSetNotExistsException {
		DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
		if (ds == null) {
			throw new DataSetNotExistsException();
		}

		dataSetDAO.removeAssignment(providerId, dataSetId, recordId, schema, versionId);
		DataProvider dataProvider = uis.getProvider(providerId);
		if (!dataSetDAO.hasMoreRepresentations(providerId, dataSetId, schema)) {
			dataSetDAO.removeRepresentationNameForDataSet(schema, providerId, dataSetId);
		}

		Representation representation = recordDAO.getRepresentation(recordId, schema, versionId);
		removeDataSetAssignmentForAllRevisions(providerId, dataSetId, recordId, schema, representation);
		representationIndexer.removeAssignment(recordId, schema,
				new CompoundDataSetId(providerId, dataSetId),
				dataProvider.getPartitionKey());

		Representation rep = recordDAO.getRepresentation(recordId, schema, versionId);
		if (rep != null) {
			for (Revision revision : rep.getRevisions())
				dataSetDAO.deleteProviderDatasetRepresentationInfo(dataSetId, providerId, recordId, schema, revision.getUpdateTimeStamp());
		}
	}

	private void removeDataSetAssignmentForAllRevisions(String providerId, String dataSetId, String recordId, String schema, Representation representation) {
		if (representation != null) {
			for (Revision revision : representation.getRevisions())
				if (revision != null)
					dataSetDAO.removeDataSetsRevision(providerId, dataSetId, RevisionUtils.getRevisionKey(revision), schema, recordId);
		}
	}

	/**
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
	public Set<String> getDataSets(String providerId, String cloudId, String representationName, String version) {
		return dataSetDAO.getDataSets(providerId, cloudId, representationName, version);
	}

	/**
     * @inheritDoc
     */
	@Override
	public void deleteDataSet(String providerId, String dataSetId)
			throws DataSetNotExistsException {
		DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);

		if (ds == null) {
			throw new DataSetNotExistsException();
		}
		dataSetDAO.deleteDataSet(providerId, dataSetId);
		DataProvider dataProvider = uis.getProvider(providerId);
		dataSetDAO.removeAllRepresentationsNamesForDataSet(providerId, dataSetId);
		representationIndexer.removeAssignmentsFromDataSet(
				new CompoundDataSetId(providerId, dataSetId),
				dataProvider.getPartitionKey());
	}

	@Override
	public Set<String> getAllDataSetRepresentationsNames(String providerId, String dataSetId) throws ProviderNotExistsException, DataSetNotExistsException {
		if (isProviderExists(providerId) && isDataSetExists(providerId, dataSetId)) {
			return dataSetDAO.getAllRepresentationsNamesForDataSet(providerId, dataSetId);
		}
		return Collections.emptySet();
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public List<String> getDataSetsRevisions(String providerId, String dataSetId, String revisionId, String representationName, String startFrom, int limit){
		if(startFrom == null){
			return dataSetDAO.getDataSetsRevision(providerId, dataSetId, revisionId, representationName, limit);
		}else{
			return dataSetDAO.getDataSetsRevisionWithPagination(providerId,dataSetId,revisionId,representationName,startFrom,limit);
		}
	}

	/**
	 * @inheritDoc
	 */
	@Override
	public void addDataSetsRevisions(String providerId, String dataSetId, String revisionId,
									 String representationName, String cloudId)
			throws ProviderNotExistsException{
		if (uis.getProvider(providerId) == null) {
			throw new ProviderNotExistsException();
		}
		dataSetDAO.addDataSetsRevision(providerId, dataSetId, revisionId, representationName, cloudId);
	}

	@Override
	public ResultSlice<CloudVersionRevisionResponse> getDataSetCloudIdsByRepresentationPublished(String dataSetId, String providerId, String representationName, Date dateFrom, String startFrom, int numberOfElementsPerPage)
			throws ProviderNotExistsException, DataSetNotExistsException {
		// check whether provider exists
		if (!uis.existsProvider(providerId))
			throw new ProviderNotExistsException("Provider doesn't exist " + providerId);

		// check whether data set exists
		if (dataSetDAO.getDataSet(providerId, dataSetId) == null)
			throw new DataSetNotExistsException("Data set " + dataSetId + " doesn't exist for provider " + providerId);

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
		if (rep == null)
			throw new RepresentationNotExistsException(schema);

		// collect data sets the version is assigned to
		Collection<CompoundDataSetId> dataSets = dataSetDAO.getDataSetAssignments(globalId, schema, version);

		// now we have to insert rows for each data set
		for (CompoundDataSetId dsID : dataSets) {
			dataSetDAO.insertProviderDatasetRepresentationInfo(dsID.getDataSetId(), dsID.getDataSetProviderId(),
					globalId, version, schema, RevisionUtils.getRevisionKey(revision), revision.getUpdateTimeStamp(),
					revision.isAcceptance(), revision.isPublished(), revision.isDeleted());
		}
	}

	private boolean isProviderExists(String providerId) throws ProviderNotExistsException {
		if (!uis.existsProvider(providerId))
			throw new ProviderNotExistsException();
		return true;
	}

	private boolean isDataSetExists(String providerId, String dataSetId) throws DataSetNotExistsException {
		DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);

		if (ds == null) {
			throw new DataSetNotExistsException();
		}
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

}
