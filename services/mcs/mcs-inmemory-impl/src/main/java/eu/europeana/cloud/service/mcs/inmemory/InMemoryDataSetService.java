package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
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
    private UISClientHandler uisHandler;


    InMemoryDataSetService(InMemoryDataSetDAO dataSetDAO, InMemoryRecordDAO recordDAO, UISClientHandler dataProviderDao) {
        super();
        this.dataSetDAO = dataSetDAO;
        this.recordDAO = recordDAO;
        this.uisHandler = dataProviderDao;
    }


    public InMemoryDataSetService() {
        super();
    }


    @Override
    public ResultSlice<Representation> listDataSet(String providerId, String dataSetId, String thresholdParam, int limit)
            throws DataSetNotExistsException {
        int treshold = 0;
        if (thresholdParam != null) {
            treshold = parseInteger(thresholdParam);
        }
        List<Representation> listOfAllStubs = dataSetDAO.listDataSet(providerId, dataSetId);
        if (listOfAllStubs.size() != 0 && treshold >= listOfAllStubs.size()) {
            throw new IllegalArgumentException("Illegal threshold param value: '" + thresholdParam + "'.");
        }
        int newOffset = -1;
        List<Representation> listOfStubs = listOfAllStubs;
        if (limit > 0) {
            listOfStubs = listOfAllStubs.subList(treshold, Math.min(treshold + limit, listOfAllStubs.size()));
            if (listOfAllStubs.size() > treshold + limit) {
                newOffset = treshold + limit;
            }
        }
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
        return newOffset == -1 ? new ResultSlice<>(null, toReturn) : new ResultSlice<>(Integer.toString(newOffset),
                toReturn);
    }


    private int parseInteger(String thresholdParam) {
        int offset = 0;
        try {
            offset = Integer.parseInt(thresholdParam);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Illegal value of threshold. It should be integer, but was '"
                    + thresholdParam + "'. ");
        }
        return offset;
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
        if (!uisHandler.providerExistsInUIS(providerId)) {
            throw new ProviderNotExistsException((String.format("Provider %s does not exist.", providerId)));
        }

        return dataSetDAO.createDataSet(providerId, dataSetId, description);
    }


    @Override
    public ResultSlice<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit) {
        if (thresholdDatasetId != null) {
            throw new UnsupportedOperationException("Paging with threshold is not supported");
        }
        // only to check if dataprovider exists
        List<DataSet> dataSets = dataSetDAO.getDataSets(providerId);
        dataSets = dataSets.subList(0, Math.min(limit, dataSets.size()));
        return new ResultSlice<>(null, dataSets);
    }


    @Override
    public void deleteDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException {
        dataSetDAO.deleteDataSet(providerId, dataSetId);
    }


    @Override
    public DataSet updateDataSet(String providerId, String dataSetId, String description)
            throws DataSetNotExistsException {
        return dataSetDAO.updateDataSet(providerId, dataSetId, description);
    }
}
