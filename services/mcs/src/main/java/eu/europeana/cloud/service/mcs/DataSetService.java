package eu.europeana.cloud.service.mcs;

import java.util.List;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

/**
 * DataSetService
 */
public interface DataSetService {

    List<Representation> listDataSet(String providerId, String dataSetId);


    void addAssignment(String providerId, String dataSetId, String recordId, String representationName, String version)
            throws DataSetNotExistsException, RepresentationNotExistsException;


    void removeAssignment(String providerId, String dataSetId, String recordId, String representationName, String version)
            throws DataSetNotExistsException;


    DataSet createDataSet(String providerId, String dataSetId, String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException;


    List<DataSet> getDataSets(String providerId)
            throws ProviderNotExistsException;


    public void deleteDataSet(String providerId, String dataSetId)
            throws ProviderNotExistsException, DataSetNotExistsException;
}
