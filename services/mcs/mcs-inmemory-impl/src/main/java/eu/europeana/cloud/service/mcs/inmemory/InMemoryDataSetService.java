package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

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
    public ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit)
            throws DataSetNotExistsException {
        if (thresholdParam != null) {
            throw new UnsupportedOperationException("Paging with threshold is not supported");
        }
        List<Representation> listOfStubs = dataSetDAO.listDataSet(providerId, dataSetId);
        listOfStubs = listOfStubs.subList(0, Math.min(limit, listOfStubs.size()));
        List<Representation> toReturn = new ArrayList<>(listOfStubs.size());
        for (Representation stub : listOfStubs) {
            Representation realContent;
            try {
                realContent = recordDAO.getRepresentation(stub.getRecordId(), stub.getSchema(), stub.getVersion());
            } catch (RepresentationNotExistsException e) {
                // we have reference to an object that not exists anymore!
                continue;
            }
            toReturn.add(realContent);
        }
        return new ResultSlice<>(null, toReturn);
    }


    @Override
    public void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
            throws DataSetNotExistsException, RepresentationNotExistsException {
        // just to check if such representation does exist
        recordDAO.getRepresentation(recordId, schema, version);
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
        // only to check if dataprovider exists
        dataProviderDao.getProvider(providerId);

        return dataSetDAO.createDataSet(providerId, dataSetId, description);
    }


    @Override
    public ResultSlice<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
            throws ProviderNotExistsException {
        if (thresholdDatasetId != null) {
            throw new UnsupportedOperationException("Paging with threshold is not supported");
        }
        // only to check if dataprovider exists
        dataProviderDao.getProvider(providerId);
        List<DataSet> dataSets = dataSetDAO.getDataSets(providerId);
        dataSets = dataSets.subList(0, Math.min(limit, dataSets.size()));
        return new ResultSlice<>(null, dataSets);
    }


    @Override
    public void deleteDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException {
        dataSetDAO.deleteDataSet(providerId, dataSetId);
    }
}
