package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSetException;

/**
 * InMemoryDataSetService
 */
@Service
public class InMemoryDataSetService implements DataSetService {

    @Autowired
    private InMemoryDataSetDAO dataSetDAO;

    @Autowired
    private InMemoryRecordDAO recordDAO;

    @Autowired
    private InMemoryDataProviderDAO dataProviderDao;


    @Override
    public List<Representation> listDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException {
        List<Representation> listOfStubs = dataSetDAO.listDataSet(providerId, dataSetId);
        List<Representation> toReturn = new ArrayList<>(listOfStubs.size());
        for (Representation stub : listOfStubs) {
            Representation realContent;
            try {
                realContent = recordDAO.getRepresentation(stub.getRecordId(), stub.getSchema(), stub.getVersion());
            } catch (RecordNotExistsException | RepresentationNotExistsException | VersionNotExistsException e) {
                // we have reference to an object that not exists anymore!
                continue;
            }
            toReturn.add(realContent);
        }
        return toReturn;
    }


    @Override
    public void addAssignment(String providerId, String dataSetId, String recordId, String representationName, String version)
            throws DataSetNotExistsException, RepresentationNotExistsException, RepresentationAlreadyInSetException {
        // just to check if such representation does exist
        recordDAO.getRepresentation(recordId, representationName, version);
        dataSetDAO.addAssignment(providerId, dataSetId, recordId, representationName, version);
    }


    @Override
    public void removeAssignment(String providerId, String dataSetId, String recordId, String representationName)
            throws DataSetNotExistsException {
        dataSetDAO.removeAssignment(providerId, dataSetId, recordId, representationName);
    }


    @Override
    public DataSet createDataSet(String providerId, String dataSetId, String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException {
        // only to check if dataprovider exists
        dataProviderDao.getProvider(providerId);

        return dataSetDAO.createDataSet(providerId, dataSetId, description);
    }


    @Override
    public List<DataSet> getDataSets(String providerId)
            throws ProviderNotExistsException {
        // only to check if dataprovider exists
        dataProviderDao.getProvider(providerId);

        return dataSetDAO.getDataSets(providerId);
    }


    @Override
    public void deleteDataSet(String providerId, String dataSetId)
            throws ProviderNotExistsException, DataSetNotExistsException {
        // only to check if dataprovider exists
        dataProviderDao.getProvider(providerId);
        dataSetDAO.deleteDataSet(providerId, dataSetId);
    }
}
