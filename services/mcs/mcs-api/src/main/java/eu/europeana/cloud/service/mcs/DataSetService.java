package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

/**
 * Service for data sets and representation assignments to data sets.
 */
public interface DataSetService {

	/**
	 * Returns all representations from particular data set (in slices). If data set contains representation not in
	 * specified version, the latest persistent representation version will be returned.
	 *
	 * @param providerId provider's (owner of data set) id.
	 * @param dataSetId data set id
	 * @param thresholdParam if null - will return first result slice. Result slices contain token for next pages, which
	 * should be provided in this parameter.
	 * @param limit max number of results in one slice.
	 * @throws DataSetNotExistsException dataset not exists.
	 * @return list of representations as a result slice.
	 */
	ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit)
			throws DataSetNotExistsException;


	/**
	 * Assigns a representation in predefined or latest version to a data set. Temporary representation may be added to
	 * a data set only if version is provided. If the same representation was already assigned to a data set, version of
	 * representation will be overwritten by provided in this method.
	 *
	 * @param providerId owner of data set
	 * @param dataSetId data set id
	 * @param recordId id of record
	 * @param schema schema name of representation
	 * @param version version of representatnion (if null, the latest persistent version is assigned to a data set)
	 * @throws DataSetNotExistsException if such data set not exists
	 * @throws RepresentationNotExistsException if such representation does not exist. May be also thrown if version is
	 * not provided and no persistent representation version exist for specified schema and record.
	 */
	void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
			throws DataSetNotExistsException, RepresentationNotExistsException;


	/**
	 * Removes representation assignment from data set.
	 *
	 * @param providerId owner of data set
	 * @param dataSetId data set id
	 * @param recordId id of record
	 * @param schema schema name of representation
	 * @throws DataSetNotExistsException if such data set not exists
	 */
	void removeAssignment(String providerId, String dataSetId, String recordId, String schema)
			throws DataSetNotExistsException;


	DataSet createDataSet(String providerId, String dataSetId, String description)
			throws ProviderNotExistsException, DataSetAlreadyExistsException;


	/**
	 * Returns all data sets for particular data provider (in slices).
	 *
	 * @param providerId provider id.
	 * @param thresholdDatasetId if null - will return first result slice. Result slices contain token for next pages,
	 * which should be provided in this parameter.
	 * @param limit max number of results in one slice.
	 * @return list of data sets as a result slice.
	 * @throws ProviderNotExistsException no such data provider exists.
	 */
	public ResultSlice<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
			throws ProviderNotExistsException;


	/**
	 * Deletes data set.
	 *
	 * @param providerId provider id
	 * @param dataSetId data set id.
	 * @throws DataSetNotExistsException no such data set exists (data provider does not have one or data provider not
	 * exist)
	 */
	public void deleteDataSet(String providerId, String dataSetId)
			throws DataSetNotExistsException;
}
