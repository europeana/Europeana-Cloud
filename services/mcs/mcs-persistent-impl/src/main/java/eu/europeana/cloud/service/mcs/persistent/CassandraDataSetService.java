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
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSetException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.nio.charset.Charset;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author sielski
 */
public class CassandraDataSetService implements DataSetService {

	@Autowired
	private CassandraDataSetDAO dataSetDAO;
	
	@Autowired
	private CassandraRecordDAO recordDAO;
	
	@Autowired
	private CassandraDataProviderDAO dataProviderDAO;


	@Override
	public ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit) {
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
		List<Representation> representations = dataSetDAO.
				listDataSet(providerId, dataSetId, thresholdCloudId, thresholdSchemaId, limit + 1);
		String nextResultToken = null;
		if (representations.size() == limit + 1) {
			Representation nextResult = representations.get(limit);
			nextResultToken = encodeParams(nextResult.getRecordId(), nextResult.getSchema());
			representations.remove(limit);
		}
		return new ResultSlice(nextResultToken, representations);
	}


	@Override
	public void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
			throws DataSetNotExistsException, RepresentationNotExistsException, RepresentationAlreadyInSetException {
		
		// check if dataset exists
		dataSetDAO.getDataSet(providerId, dataSetId);
		
		// check if representation exists
		recordDAO.getRepresentation(recordId, schema, version);
		
		// now - if everyting exists - add assignment
		dataSetDAO.addAssignment(providerId, dataSetId, recordId, schema, version);
	}


	@Override
	public void removeAssignment(String providerId, String dataSetId, String recordId, String schema)
			throws DataSetNotExistsException {
		dataSetDAO.removeAssignment(providerId, dataSetId, recordId, schema);
	}


	@Override
	public DataSet createDataSet(String providerId, String dataSetId, String description)
			throws ProviderNotExistsException, DataSetAlreadyExistsException {
		return dataSetDAO.createDataSet(providerId, dataSetId, description);
	}


	@Override
	public ResultSlice<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
			throws ProviderNotExistsException {

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


	@Override
	public void deleteDataSet(String providerId, String dataSetId)
			throws ProviderNotExistsException, DataSetNotExistsException {
		dataSetDAO.deleteDataSet(providerId, dataSetId);
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
