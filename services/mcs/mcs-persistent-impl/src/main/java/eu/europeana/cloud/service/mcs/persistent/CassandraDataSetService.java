package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Implementation of data set service using Cassandra database.
 */
public class CassandraDataSetService implements DataSetService {

	@Autowired
	private CassandraDataSetDAO dataSetDAO;

	@Autowired
	private CassandraRecordDAO recordDAO;

	@Autowired
	private CassandraDataProviderDAO dataProviderDAO;


	/**
	 * @inheritDoc
	 */
	@Override
	public ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit)
			throws DataSetNotExistsException {
		// check if dataset exists
		DataSet ds;
		try {
			ds = dataSetDAO.getDataSet(providerId, dataSetId);
		} catch (ProviderNotExistsException ex) {
			throw new DataSetNotExistsException("No such provider: " + providerId);
		}
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
		List<Representation> representationStubs = dataSetDAO.
				listDataSet(providerId, dataSetId, thresholdCloudId, thresholdSchemaId, limit + 1);

		// if this is not last slice of result - add reference to next one by encoding parameters in thresholdParam
		String nextResultToken = null;
		if (representationStubs.size() == limit + 1) {
			Representation nextResult = representationStubs.get(limit);
			nextResultToken = encodeParams(nextResult.getRecordId(), nextResult.getSchema());
			representationStubs.remove(limit);
		}

		// replace representation stubs with real representations
		List<Representation> representations = new ArrayList<>(representationStubs.size());
		for (Representation stub : representationStubs) {
			if (stub.getVersion() == null) {
				representations.add(recordDAO.getLatestPersistentRepresentation(stub.getRecordId(), stub.getSchema()));
			} else {
				representations.
						add(recordDAO.getRepresentation(stub.getRecordId(), stub.getSchema(), stub.getVersion()));
			}
		}
		return new ResultSlice(nextResultToken, representations);
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
			throws DataSetNotExistsException, RepresentationNotExistsException {

		// check if dataset exists
		DataSet ds;
		try {
			ds = dataSetDAO.getDataSet(providerId, dataSetId);
		} catch (ProviderNotExistsException ex) {
			throw new DataSetNotExistsException("No such provider: " + providerId);
		}
		if (ds == null) {
			throw new DataSetNotExistsException();
		}

		// check if representation exists
		if (version == null) {
			Representation rep = recordDAO.getLatestPersistentRepresentation(recordId, schema);
			if (rep == null) {
				throw new RepresentationNotExistsException();
			}
		} else {
			Representation rep = recordDAO.getRepresentation(recordId, schema, version);

			if (rep == null) {
				throw new RepresentationNotExistsException();
			}
		}

		// now - when everything is validated - add assignment
		dataSetDAO.addAssignment(providerId, dataSetId, recordId, schema, version);
		// TODO: add asignment in SOLR
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public void removeAssignment(String providerId, String dataSetId, String recordId, String schema)
			throws DataSetNotExistsException {
		// check if dataset exists
		DataSet ds;
		try {
			ds = dataSetDAO.getDataSet(providerId, dataSetId);
		} catch (ProviderNotExistsException ex) {
			throw new DataSetNotExistsException("No such provider: " + providerId);
		}
		if (ds == null) {
			throw new DataSetNotExistsException();
		}

		dataSetDAO.removeAssignment(providerId, dataSetId, recordId, schema);
		// TODO: remove asignment in SOLR
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public DataSet createDataSet(String providerId, String dataSetId, String description)
			throws ProviderNotExistsException, DataSetAlreadyExistsException {
		// check if data provider exists
		if (dataProviderDAO.getProvider(providerId) == null) {
			throw new ProviderNotExistsException();
		}

		// check if dataset exists
		DataSet ds = dataSetDAO.getDataSet(providerId, dataSetId);
		if (ds != null) {
			throw new DataSetAlreadyExistsException();
		}

		return dataSetDAO.createDataSet(providerId, dataSetId, description);
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public ResultSlice<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
			throws ProviderNotExistsException {
		// check if data provider exists
		if (dataProviderDAO.getProvider(providerId) == null) {
			throw new ProviderNotExistsException();
		}

		List<DataSet> dataSets = dataSetDAO.
				getDataSets(providerId, thresholdDatasetId, limit + 1);
		String nextDataSet = null;
		if (dataSets.size() == limit + 1) {
			DataSet nextResult = dataSets.get(limit);
			nextDataSet = nextResult.getId();
			dataSets.remove(limit);
		}
		return new ResultSlice(nextDataSet, dataSets);
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public void deleteDataSet(String providerId, String dataSetId)
			throws DataSetNotExistsException {
		dataSetDAO.deleteDataSet(providerId, dataSetId);
		// TODO: remove asignments in SOLR
	}


	private String encodeParams(String... parts) {
		byte[] paramsJoined = Joiner.on('\n').join(parts).getBytes(Charset.forName("UTF-8"));
		return BaseEncoding.base32().encode(paramsJoined);
	}


	private List<String> decodeParams(String encodedParams) {
		byte[] paramsDecoded = BaseEncoding.base32().decode(encodedParams);
		String paramsDecodedString = new String(paramsDecoded, Charset.forName("UTF-8"));
		return Splitter.on('\n').splitToList(paramsDecodedString);
	}

}
