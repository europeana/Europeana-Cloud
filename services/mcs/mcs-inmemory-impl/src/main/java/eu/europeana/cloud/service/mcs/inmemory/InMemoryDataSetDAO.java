package eu.europeana.cloud.service.mcs.inmemory;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Repository;

/**
 * InMemoryDataSetDAO
 */
@Repository
public class InMemoryDataSetDAO {

    // providerId -> (datasetId -> dataSet)
    private Map<String, Map<String, DataSet>> dataSets = new HashMap<>();

    private Map<DataSet, List<Representation>> dataSetsAssignments = new HashMap<>();


    public List<Representation> listDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException {
        DataSet dataSet = getDataSet(providerId, dataSetId);
        if (dataSet == null) {
            throw new DataSetNotExistsException();
        }
        List<Representation> listOfStubs = dataSetsAssignments.get(dataSet);
        return listOfStubs;
    }


    public List<DataSet> getAllByProviderId(String providerId) {
        Map<String, DataSet> allForProvider = dataSets.get(providerId);
        if (allForProvider != null) {
            return new ArrayList<>(allForProvider.values());
        } else {
            return new ArrayList<>(0);
        }
    }


    private DataSet getDataSet(String providerId, String dataSetId) {
        Map<String, DataSet> idToDataset = dataSets.get(providerId);
        if (idToDataset != null) {
            return idToDataset.get(dataSetId);
        }
        return null;
    }


    public void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
            throws DataSetNotExistsException, RepresentationNotExistsException {
        DataSet dataSet = getDataSet(providerId, dataSetId);
        if (dataSet == null) {
            throw new DataSetNotExistsException();
        }
        // just to check if such representation does exist
        List<Representation> listOfStubs = dataSetsAssignments.get(dataSet);
        Representation stub = getStub(listOfStubs, recordId, schema);
        if (stub == null) {
            stub = new Representation();
            stub.setRecordId(recordId);
            stub.setSchema(schema);
            stub.setVersion(version);
            listOfStubs.add(stub);
        } else {
            stub.setVersion(version);
        }
    }


    public void removeAssignment(String providerId, String dataSetId, String recordId, String schema)
            throws DataSetNotExistsException {
        DataSet dataSet = getDataSet(providerId, dataSetId);
        if (dataSet == null) {
            throw new DataSetNotExistsException();
        }
        List<Representation> listOfStubs = dataSetsAssignments.get(dataSet);
        Representation stub = getStub(listOfStubs, recordId, schema);
        if (stub != null) {
            listOfStubs.remove(stub);
        }
    }


    private Representation getStub(List<Representation> listOfStubs, String recordId, String schema) {
        for (Representation stub : listOfStubs) {
            if (stub.getRecordId().equals(recordId) && stub.getSchema().equals(schema)) {
                return stub;
            }
        }
        return null;
    }


    public DataSet createDataSet(String providerId, String dataSetId, String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException {
        // only to check if dataprovider exists

        if (!dataSets.containsKey(providerId)) {
            dataSets.put(providerId, new HashMap<String, DataSet>());
        }
        Map<String, DataSet> providerSets = dataSets.get(providerId);
        if (providerSets.containsKey(dataSetId)) {
            throw new DataSetAlreadyExistsException();
        }
        DataSet dataSet = new DataSet();
        dataSet.setId(dataSetId);
        dataSet.setProviderId(providerId);
        dataSet.setDescription(description);
        providerSets.put(dataSetId, dataSet);
        dataSetsAssignments.put(dataSet, new ArrayList<Representation>());
        return dataSet;
    }


    public DataSet updateDataSet(String providerId, String dataSetId, String description)
            throws DataSetNotExistsException {
        DataSet dataSetToUpdate = getDataSet(providerId, dataSetId);
        if (dataSetToUpdate == null) {
            throw new DataSetNotExistsException();
        }
        dataSetToUpdate.setDescription(description);
        return dataSetToUpdate;
    }


    public List<DataSet> getDataSets(String providerId)
            throws ProviderNotExistsException {

        Map<String, DataSet> datasetsForProvider = dataSets.get(providerId);
        if (datasetsForProvider != null) {
            return new ArrayList<>(datasetsForProvider.values());
        } else {
            return new ArrayList<>(0);
        }
    }


    public void deleteDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException {

        DataSet dataSetToRemove = getDataSet(providerId, dataSetId);
        if (dataSetToRemove == null) {
            throw new DataSetNotExistsException();
        }
        dataSets.get(providerId).remove(dataSetId);
        dataSetsAssignments.remove(dataSetToRemove);
    }
}
