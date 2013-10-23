package eu.europeana.cloud.service.uis.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.GlobalId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;

/**
 * In-memory mockup of the unique identifier service
 * 
 * @see UniqueIdentifierService
 * @author Yorgos.Mamakis@kb.nl
 * @since Oct 17, 2013
 */
@Service
public class InMemoryUniqueIdentifierService implements UniqueIdentifierService {
    private List<Record>              records           = new ArrayList<>();
    private Map<String, List<String>> providerLocalIds  = new HashMap<>();
    private Map<String, List<String>> providerGlobalIds = new HashMap<>();

    @Override
    public GlobalId createGlobalId(String providerId, String recordId)
            throws DatabaseConnectionException, RecordExistsException {
        String globalId = String.format("/%s/%s", providerId, recordId);
        for (Record record : records) {
            if (StringUtils.equals(record.getId(), globalId)) { throw new RecordExistsException(); }
        }
        Record record = new Record();
        Representation rep = new Representation();
        rep.setDataProvider(providerId);
        rep.setRecordId(recordId);
        List<Representation> reps = new ArrayList<>();
        reps.add(rep);
        record.setRepresentations(reps);
        record.setId(globalId);
        records.add(record);
        List<String> recordList = providerLocalIds.get(providerId) != null
                ? providerLocalIds.get(providerId) : new ArrayList<String>();
        if (!recordList.contains(recordId)) {
            recordList.add(recordId);
        }
        providerLocalIds.put(providerId, recordList);

        List<String> globalList = providerGlobalIds.get(providerId) != null
                ? providerGlobalIds.get(providerId) : new ArrayList<String>();
        if (!globalList.contains(globalId)) {
            globalList.add(globalId);
        }
        providerGlobalIds.put(providerId, globalList);

        LocalId localId = new LocalId();
        localId.setProviderId(providerId);
        localId.setRecordId(recordId);

        GlobalId gId = new GlobalId();
        gId.setLocalId(localId);
        gId.setId(globalId);
        return gId;
    }

    @Override
    public GlobalId getGlobalId(String providerId, String recordId)
            throws DatabaseConnectionException, RecordDoesNotExistException {
        for (Record rec : records) {

            for (Representation representation : rec.getRepresentations()) {
                if (StringUtils.equals(representation.getDataProvider(), providerId) &&
                    StringUtils.equals(representation.getRecordId(), recordId)) {
                    LocalId localId = new LocalId();
                    localId.setProviderId(providerId);
                    localId.setRecordId(recordId);

                    GlobalId gId = new GlobalId();
                    gId.setLocalId(localId);
                    gId.setId(rec.getId());
                    return gId;
                }
            }
        }
        throw new RecordDoesNotExistException();
    }

    @Override
    public List<LocalId> getLocalIdsByGlobalId(String globalId) throws DatabaseConnectionException,
            GlobalIdDoesNotExistException {
        for (Record record : records) {
            if (StringUtils.equals(record.getId(), globalId)) {
                List<LocalId> localIds = new ArrayList<>();
                for (Representation rep : record.getRepresentations()) {
                    LocalId localId = new LocalId();
                    localId.setProviderId(rep.getDataProvider());
                    localId.setRecordId(rep.getRecordId());
                    localIds.add(localId);
                }
                return localIds;
            }
        }
        throw new GlobalIdDoesNotExistException();
    }

    @Override
    public List<LocalId> getLocalIdsByProvider(String providerId, int start, int end)
            throws DatabaseConnectionException, ProviderDoesNotExistException,
            RecordDatasetEmptyException {
        if (providerLocalIds.containsKey(providerId)) {
            if (providerLocalIds.get(providerId).isEmpty() ||
                providerLocalIds.get(providerId).size() < start) { throw new RecordDatasetEmptyException(); }
            List<LocalId> providers = new ArrayList<>();
            for (String localId : providerLocalIds.get(providerId).subList(start,
                    Math.min(providerLocalIds.get(providerId).size(), start + end))) {
                LocalId provider = new LocalId();
                provider.setProviderId(providerId);
                provider.setRecordId(localId);
                providers.add(provider);
            }
            return providers;
        }
        throw new ProviderDoesNotExistException();
    }

    @Override
    public List<GlobalId> getGlobalIdsByProvider(String providerId, int start, int end)
            throws DatabaseConnectionException, ProviderDoesNotExistException,
            RecordDatasetEmptyException {
        if (providerGlobalIds.containsKey(providerId)) {
            if (providerLocalIds.get(providerId).isEmpty() ||
                providerLocalIds.get(providerId).size() < start) { throw new RecordDatasetEmptyException(); }

            List<GlobalId> globalIds = new ArrayList<>();
            for (String globalId : providerGlobalIds.get(providerId).subList(start,
                    Math.min(providerGlobalIds.get(providerId).size(), start + end))) {
                LocalId provider = new LocalId();
                provider.setProviderId(providerId);

                GlobalId gId = new GlobalId();
                gId.setLocalId(provider);
                gId.setId(globalId);
                globalIds.add(gId);
            }
            return globalIds;

        }
        throw new ProviderDoesNotExistException();
    }

    @Override
    public void createIdMapping(String globalId, String providerId, String recordId)
            throws DatabaseConnectionException, ProviderDoesNotExistException,
            GlobalIdDoesNotExistException, RecordIdDoesNotExistException, IdHasBeenMappedException {
        if (!providerLocalIds.containsKey(providerId)) { throw new ProviderDoesNotExistException(); }
        if (!providerLocalIds.get(providerId).contains(recordId)) { throw new RecordIdDoesNotExistException(); }
        if (providerGlobalIds.get(providerId).contains(globalId)) { throw new IdHasBeenMappedException(); }
        for (Record record : records) {
            if (StringUtils.equals(record.getId(), globalId)) {
                Representation rep = new Representation();
                rep.setRecordId(recordId);
                rep.setDataProvider(providerId);
                record.getRepresentations().add(rep);
            }
        }
        throw new GlobalIdDoesNotExistException();
    }

    @Override
    public void removeIdMapping(String providerId, String recordId)
            throws DatabaseConnectionException, ProviderDoesNotExistException,
            RecordIdDoesNotExistException {
        // Mockup it will be soft delete in reality
        for (Record record : records) {
            for (Representation representation : record.getRepresentations()) {
                if (StringUtils.equals(representation.getDataProvider(), providerId) &&
                    StringUtils.equals(recordId, representation.getRecordId())) {
                    records.remove(record);
                }
            }
        }
    }

    @Override
    public void deleteGlobalId(String globalId) throws DatabaseConnectionException,
            GlobalIdDoesNotExistException {
        // Mockup it will be soft delete in reality
        for (Record record : records) {
            if (StringUtils.equals(record.getId(), globalId)) {
                records.remove(record);
            }
        }
        throw new GlobalIdDoesNotExistException();
    }
}
