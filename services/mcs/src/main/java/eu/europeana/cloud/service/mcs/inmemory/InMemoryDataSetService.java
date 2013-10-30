package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
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

/**
 * InMemoryDataSetService
 */
@Service
public class InMemoryDataSetService implements DataSetService {

    // providerId, datasetId -> dataSet
    private Map<String, Map<String, DataSet>> dataSets = new HashMap<>();

    private Map<DataSet, List<Representation>> dataSetsAssignments = new HashMap<>();

    @Autowired
    private RecordService recordService;

    @Autowired
    private DataProviderService dataProviderService;


    @Override
    public List<Representation> listDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException {
        DataSet dataSet = getDataSet(providerId, dataSetId);
        if (dataSet == null) {
            throw new DataSetNotExistsException();
        }
        List<Representation> listOfStubs = dataSetsAssignments.get(dataSet);
        List<Representation> toReturn = new ArrayList<>(listOfStubs.size());
        for (Representation stub : listOfStubs) {
            Representation realContent;
            try {
                realContent = recordService.getRepresentation(stub.getRecordId(), stub.getSchema(), stub.getVersion());
            } catch (RecordNotExistsException | RepresentationNotExistsException | VersionNotExistsException e) {
                // we have reference to an object that not exists anymore!
                continue;
            }
            toReturn.add(realContent);
        }
        return toReturn;
    }


    private DataSet getDataSet(String providerId, String dataSetId) {
        Map<String, DataSet> idToDataset = dataSets.get(providerId);
        if (idToDataset != null) {
            return idToDataset.get(dataSetId);
        }
        return null;
    }


    @Override
    public void addAssignment(String providerId, String dataSetId, String recordId, String representationName, String version)
            throws DataSetNotExistsException, RepresentationNotExistsException {
        DataSet dataSet = getDataSet(providerId, dataSetId);
        if (dataSet == null) {
            throw new DataSetNotExistsException();
        }
        // just to check if such representation does exist
        recordService.getRepresentation(recordId, representationName, version);
        List<Representation> listOfStubs = dataSetsAssignments.get(dataSet);
        Representation stub = getStub(listOfStubs, recordId, representationName);
        if (stub == null) {
            stub = new Representation();
            stub.setRecordId(recordId);
            stub.setSchema(representationName);
            stub.setVersion(version);
            listOfStubs.add(stub);
        }
    }


    @Override
    public void removeAssignment(String providerId, String dataSetId, String recordId, String representationName, String version)
            throws DataSetNotExistsException {
        DataSet dataSet = getDataSet(providerId, dataSetId);
        if (dataSet == null) {
            throw new DataSetNotExistsException();
        }
        List<Representation> listOfStubs = dataSetsAssignments.get(dataSet);
        Representation stub = getStub(listOfStubs, recordId, representationName);
        if (stub != null) {
            listOfStubs.remove(stub);
        }
    }


    private Representation getStub(List<Representation> listOfStubs, String recordId, String representationName) {
        for (Representation stub : listOfStubs) {
            if (stub.getRecordId().equals(recordId) && stub.getSchema().equals(representationName)) {
                return stub;
            }
        }
        return null;
    }


    @Override
    public DataSet createDataSet(String providerId, String dataSetId)
            throws ProviderNotExistsException, DataSetAlreadyExistsException {
        // only to check if dataprovider exists
        dataProviderService.getProvider(providerId);

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
        providerSets.put(dataSetId, dataSet);
        dataSetsAssignments.put(dataSet, new ArrayList<Representation>());
        return dataSet;
    }


    @Override
    public List<DataSet> getDataSets(String providerId)
            throws ProviderNotExistsException {
        // only to check if dataprovider exists
        dataProviderService.getProvider(providerId);

        // ??
        List<DataSet> sets = new ArrayList<>();
        Collection<Map<String, DataSet>> values = dataSets.values();
        Iterator<Map<String, DataSet>> iterator = values.iterator();
        while (iterator.hasNext()) {
            sets.addAll(iterator.next().values());
        }
        return sets;
    }


    @Override
    public void deleteDataSet(String providerId, String dataSetId)
            throws ProviderNotExistsException, DataSetNotExistsException {
        // only to check if dataprovider exists
        dataProviderService.getProvider(providerId);

        DataSet dataSetToRemove = getDataSet(providerId, dataSetId);
        if (dataSetToRemove == null) {
            throw new DataSetNotExistsException();
        }
        dataSets.get(providerId).remove(dataSetId);
        dataSetsAssignments.remove(dataSetToRemove);
    }
}
