package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.springframework.stereotype.Repository;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;

/**
 * InMemoryRecordDAO
 */
@Repository
public class InMemoryRecordDAO {

    // globalId -> (representationName -> representationVersions)
    private Map<String, Map<String, List<Representation>>> records = new HashMap<>();


    public Record getRecord(String globalId)
            throws RecordNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RecordNotExistsException(globalId);
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


    public void deleteRecord(String globalId)
            throws RecordNotExistsException {
        if (records.containsKey(globalId)) {
            records.remove(globalId);
        } else {
            throw new RecordNotExistsException(globalId);
        }
    }


    public List<Representation> findRepresentations(String providerId, String schema) {
        List<Representation> representations = new ArrayList<>();
        for (Map<String, List<Representation>> representationNameToVersionList : records.values()) {
            for (List<Representation> repVersionList : representationNameToVersionList.values()) {
                Representation latestVersion = getLatestRepresentation(repVersionList);
                boolean providerMatch = providerId == null || latestVersion.getDataProvider().equals(providerId);
                boolean schemaMatch = schema == null || latestVersion.getSchema().equals(schema);
                if (providerMatch && schemaMatch) {
                    representations.add(latestVersion);
                }
            }
        }
        return representations;
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


    public void deleteRepresentation(String globalId, String schema)
            throws  RepresentationNotExistsException {
        Map<String, List< Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RepresentationNotExistsException();
        }
        if (representations.containsKey(schema)) {
            representations.remove(schema);
        } else {
            throw new RepresentationNotExistsException();
        }
    }


    public Representation createRepresentation(String globalId, String schema, String providerId) {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            representations = new HashMap<>();
            records.put(globalId, representations);
        }
        List<Representation> representationVersions = representations.get(schema);
        if (representationVersions == null) {
            representationVersions = new ArrayList<>();
            representations.put(schema, representationVersions);
        }
        Representation rep = new Representation();
        rep.setRecordId(globalId);
        rep.setPersistent(false);
        rep.setDataProvider(providerId);
        rep.setSchema(schema);
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


    public Representation getRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, VersionNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RepresentationNotExistsException("No representation for " + globalId);
        }
        List<Representation> representationVersions = representations.get(schema);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        if (version == null) {
            return getLatestRepresentation(representationVersions);
        } else {
            Representation rep = getByVersion(representationVersions, version);
            if (rep == null) {
                throw new VersionNotExistsException();
            } else {
                return copy(rep);
            }
        }
    }


    public void deleteRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RepresentationNotExistsException(globalId);
        }
        List<Representation> representationVersions = representations.get(schema);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        Representation repInVersion = getByVersion(representationVersions, version);
        if (repInVersion == null) {
            throw new VersionNotExistsException();
        } else if (repInVersion.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
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


    public Representation persistRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RepresentationNotExistsException(globalId);
        }
        List<Representation> representationVersions = representations.get(schema);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        Representation repVersion = getByVersion(representationVersions, version);
        if (repVersion == null) {
            throw new VersionNotExistsException();
        } else if (repVersion.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException("Representation " + globalId + " - " + schema + " - " + version + " is already persistent");
        }
        String newVersion = generateNewVersionNumber(representationVersions, true);
        repVersion.setVersion(newVersion);
        repVersion.setPersistent(true);

        representationVersions.remove(repVersion);
        representationVersions.add(repVersion);
        return repVersion;
    }


    public List<Representation> listRepresentationVersions(String globalId, String schema)
            throws RepresentationNotExistsException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RepresentationNotExistsException(globalId);
        }
        List<Representation> representationVersions = representations.get(schema);
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


    public Representation addOrReplaceFileInRepresentation(String globalId, String schema, String version, File file)
            throws RepresentationNotExistsException, FileAlreadyExistsException, CannotModifyPersistentRepresentationException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RepresentationNotExistsException(globalId);
        }
        List<Representation> representationVersions = representations.get(schema);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        Representation rep = getByVersion(representationVersions, version);
        if (rep == null) {
            throw new VersionNotExistsException();
        }

        if (rep.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
        }

        ListIterator<File> filesIterator = rep.getFiles().listIterator();
        while (filesIterator.hasNext()) {
            File f = filesIterator.next();
            if (f.getFileName().equals(file.getFileName())) {
                filesIterator.remove();
                break;
            }
        }

        rep.getFiles().add(file);
        return copy(rep);
    }


    public Representation removeFileFromRepresentation(String globalId, String schema, String version, String fileName)
            throws RepresentationNotExistsException, VersionNotExistsException, FileNotExistsException, CannotModifyPersistentRepresentationException {
        Map<String, List<Representation>> representations = records.get(globalId);
        if (representations == null) {
            throw new RepresentationNotExistsException(globalId);
        }
        List<Representation> representationVersions = representations.get(schema);
        if (representationVersions == null) {
            throw new RepresentationNotExistsException();
        }
        Representation rep = getByVersion(representationVersions, version);
        if (rep == null) {
            throw new VersionNotExistsException();
        }

        if (rep.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
        }

        for (File f : rep.getFiles()) {
            if (f.getFileName().equals(fileName)) {
                rep.getFiles().remove(f);
                return copy(rep);
            }
        }
        throw new FileNotExistsException();
    }
}
