package eu.europeana.cloud.service.mcs;

import java.util.List;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSetException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

/**
 * DataSetService
 */
public interface DataSetService {

    ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit);


    void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
            throws DataSetNotExistsException, RepresentationNotExistsException, RepresentationAlreadyInSetException;


    void removeAssignment(String providerId, String dataSetId, String recordId, String schema)
            throws DataSetNotExistsException;


    DataSet createDataSet(String providerId, String dataSetId, String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException;


    public ResultSlice<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
            throws ProviderNotExistsException;


    public void deleteDataSet(String providerId, String dataSetId)
            throws ProviderNotExistsException, DataSetNotExistsException;
}
