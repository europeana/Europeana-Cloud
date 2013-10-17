package eu.europeana.cloud.service.mcs.service.inmemory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotDeletePersistentRepresentationVersion;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyPersistentException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.service.RecordService;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryRecordService implements RecordService {
    
    private Map<String, Map<String, List<Representation>>> records = new HashMap<>();
    
    
    @Override
    public Record getRecord(String globalId)
            throws RecordNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException();
        }
        Record record = new Record();
        record.setId(globalId);
        List<Representation> representationInfos = new ArrayList<>();
        record.setRepresentations(representationInfos);
        for (Map.Entry<String, List<Representation>> representation : representations.entrySet()) {
            representationInfos.add(getLatestRepresentation(representation.getValue()));
        }
        return record;
    }
    
    
    @Override
    public void deleteRecord(String globalId)
            throws RecordNotExistsException {
        if (records.containsKey(globalId)) {
            records.remove(globalId);
        } else {
            throw new RecordNotExistsException();
        }
    }
    
    
    private Representation getLatestPersistentRepresentation(List<Representation> versions) {
        
        for (int i = versions.size() - 1; i >= 0; i--) {
            Representation rep = versions.get(i);
            if (rep.isPersistent()) {
                return copy(rep);
            }
        }
        return null;
    }
    
    
    private Representation getLatestRepresentation(List<Representation> versions) {
        Representation latestPersistent = getLatestPersistentRepresentation(versions);
        if (latestPersistent == null) {
            return copy(versions.get(versions.size() - 1));
        } else {
            return latestPersistent;
        }
    }
    
    
    private Representation copy(Representation rep) {
        Representation copy = new Representation();
        copy.setDataProvider(rep.getDataProvider());
        copy.setFiles(new ArrayList<>(rep.getFiles()));
        copy.setPersistent(rep.isPersistent());
        copy.setRecordId(rep.getRecordId());
        copy.setSchema(rep.getSchema());
        copy.setVersion(rep.getVersion());
        return copy;
    }
    
    
    @Override
    public Representation getRepresentation(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException();
        }
        List<Representation> representationVersions = representations.get(representationName);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        return getLatestRepresentation(representationVersions);
    }
    
    
    @Override
    public void deleteRepresentation(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException {
        Map<String, List< Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException();
        }
        if (representations.containsKey(representationName)) {
            representations.remove(representationName);
        } else {
            throw new RepresentationNotExistsException();
        }
    }
    
    
    @Override
    public Representation createRepresentation(String globalId, String representationName, String providerId) {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            representations = new HashMap<>();
            records.put(globalId, representations);
        }
        List<Representation> representationVersions = representations.get(representationName);
        if (representationVersions == null) {
            representationVersions = new ArrayList<>();
            representations.put(representationName, representationVersions);
        }
        Representation rep = new Representation();
        rep.setRecordId(globalId);
        rep.setPersistent(false);
        rep.setDataProvider(providerId);
        rep.setSchema(representationName);
        rep.setFiles(new ArrayList<File>());
        String version = generateNewVersionNumber(representationVersions, false);
        rep.setVersion(version);
        representationVersions.add(rep);
        return rep;
    }

// persistentVersion: \d+
    // tempVersion: \d+[.]PRE-\d+

    private String generateNewVersionNumber(List<Representation> versions, boolean persistent) {
        if (versions.isEmpty()) {
            if (persistent) {
                return "1";
            } else {
                return "1.PRE-1";
            }
        }
        Representation latestRepresentation = versions.get(versions.size() - 1);
        boolean latestIsPersistent = latestRepresentation.isPersistent();
        if (latestIsPersistent && persistent) {
            return Integer.toString(Integer.parseInt(latestRepresentation.getVersion()) + 1);
        } else if (latestIsPersistent && !persistent) {
            return Integer.toString(Integer.parseInt(latestRepresentation.getVersion()) + 1) + ".PRE-1";
        } else if (persistent) {
            return latestRepresentation.getVersion().substring(0, latestRepresentation.getVersion().indexOf("."));
        } else {
            String[] parts = latestRepresentation.getVersion().split("-");
            return parts[0] + "-" + (Integer.parseInt(parts[1]) + 1);
        }
    }
    
    
    @Override
    public Representation getRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException();
        }
        List<Representation> representationVersions = representations.get(representationName);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        Representation rep = getByVersion(representationVersions, version);
        if (rep == null) {
            throw new VersionNotExistsException();
        } else {
            return copy(rep);
        }
    }
    
    
    @Override
    public void deleteRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException();
        }
        List<Representation> representationVersions = representations.get(representationName);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        Representation repInVersion = getByVersion(representationVersions, version);
        if (repInVersion == null) {
            throw new VersionNotExistsException();
        } else if (repInVersion.isPersistent()) {
            throw new CannotDeletePersistentRepresentationVersion();
        } else {
            representationVersions.remove(repInVersion);
        }
    }
    
    
    private Representation getByVersion(List<Representation> versions, String version) {
        for (Representation rep : versions) {
            if (rep.getVersion().equals(version)) {
                return rep;
            }
        }
        return null;
    }
    
    
    @Override
    public Representation persistRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, RepresentationAlreadyPersistentException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException();
        }
        List<Representation> representationVersions = representations.get(representationName);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        Representation repVersion = getByVersion(representationVersions, version);
        if (repVersion == null) {
            throw new VersionNotExistsException();
        } else if (repVersion.isPersistent()) {
            throw new RepresentationAlreadyPersistentException();
        }
        String newVersion = generateNewVersionNumber(representationVersions, true);
        repVersion.setVersion(newVersion);
        repVersion.setPersistent(true);
        
        representationVersions.remove(repVersion);
        representationVersions.add(repVersion);
        return repVersion;
    }
    
    
    @Override
    public List<Representation> listRepresentationVersions(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException();
        }
        List<Representation> representationVersions = representations.get(representationName);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        List<Representation> result = new ArrayList<>(representationVersions.size());
        for (Representation representation : representationVersions) {
            result.add(copy(representation));
        }
        Collections.reverse(result);
        return result;
    }


}
